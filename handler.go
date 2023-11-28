package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

var (
	max = 10
)

//TODO handle the unknown commands
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func GetHandler(w http.ResponseWriter, r *http.Request, tree *Tree, sst *SStables) {
	key := r.URL.Query().Get("key")

	if key == "" {
		fmt.Println("key parameter is missing")
		http.Error(w, "key parameter is missing", http.StatusBadRequest)
		return
	}
	key1 := []byte(key)
	//check if the value is in the tree if not we search in the sstfiles
	value, err := tree.Get(key1)
	if err == ErrDeleted {
		fmt.Println("key not found")
		http.Error(w, "key not found", http.StatusBadRequest)
		return
	}
	if err == ErrKeynotfound {
		// search in sstfiles
		value, err := sst.Search(key1)
		if err == ErrDeleted || err == ErrKeynotfound {
			fmt.Println("key not found")
			http.Error(w, "key not found", http.StatusBadRequest)
			return
		}
		if err != nil {
			fmt.Println(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fmt.Println("Gets key:  ", string(value))
		return
	}
	fmt.Println("Gets key: ", string(value))
}

func SetHandler(w http.ResponseWriter, r *http.Request, tree *Tree, wal *Wal, sst *SStables) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}
	decoder := json.NewDecoder(r.Body)
	var t KeyValue
	err := decoder.Decode(&t)
	if err != nil {
		http.Error(w, "Error decoding JSON data: "+err.Error(), http.StatusBadRequest)
		return
	}
	key := t.Key
	value := t.Value

	if key == "" || value == "" {
		fmt.Println("key or value parameter is missing")
		http.Error(w, "key or value parameter is missing", http.StatusBadRequest)
		return
	}
	key1 := []byte(key)
	value1 := []byte(value)
	// set the value in the tree
	err1 := tree.Set(key1, value1)
	if err1 != nil {
		http.Error(w, err1.Error(), http.StatusBadRequest)
		return
	}
	entry := Entry{
		Key:     key1,
		Value:   value1,
		Command: Set,
	}
	// add the command to the wal 
	err = wal.AppendCommand(&entry)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fmt.Println("Sets key: ", string(key), " value = ", string(value))
	fmt.Println(tree.Len())

	//if the tree has reached the maximum length it needs to be flushed to disk
	if tree.Len() == max {
		fmt.Println("wa hana hna")

		if err := FlushToDisk(tree, wal, sst); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
}
func DelHandler(w http.ResponseWriter, r *http.Request, tree *Tree, wal *Wal, sst *SStables) {
	key := r.URL.Query().Get("key")

	if key == "" {
		fmt.Println("key parameter is missing")
		http.Error(w, "key parameter is missing", http.StatusBadRequest)
		return
	}
	key1 := []byte(key)
	// if the key is in the tree it will be deleted if not we search it the sstfiles
	//if the key is there then we add it to the tree as a deleted one (marker = false)
	err := tree.Del(key1)
	if err == ErrKeynotfound {
		//the key is not in the tree we should search if it in the sstfiles
		value, err := sst.Search(key1)
		if err == ErrDeleted || err == ErrKeynotfound {
			fmt.Println("key not found")
			http.Error(w, "key not found", http.StatusBadRequest)
			return
		}
	
		// if we found it we will add it to the tree as a deleted key and to the wal  to keep it updated
		entry := Entry{
			Key:     key1,
			Value:   value,
			Command: Set,
		}
		err1 := wal.AppendCommand(&entry)
		if err1 != nil {
			http.Error(w, err1.Error(), http.StatusBadRequest)
			return
		}
		//add it to the tree but the marker will be false
		err = tree.SetDeletedKey(key1, value)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	} else if err != nil {
		//the key is already deleted in the tree
		fmt.Println("key not found")
		http.Error(w, "key not found", http.StatusBadRequest)
		return
	}
	entry := Entry{
		Key:     key1,
		Command: Del,
	}
	err1 := wal.AppendCommand(&entry)
	if err1 != nil {
		http.Error(w, err1.Error(), http.StatusBadRequest)
		return
	}
	fmt.Println("the deleted key: ", string(key))
	//if the tree has reached the maximum length it needs to be flushed to disk

	if tree.Len() == max {
		if err := FlushToDisk(tree, wal, sst); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

}

// The FlushToDisk function flushes the tree to disk, reinitializes the tree, and add 
// the watermark in the wal 
func FlushToDisk(tree *Tree, wal *Wal, sst *SStables) error {
	err := sst.Flush(tree)
	if err != nil {
		return err
	}
	err = tree.Reinitialize()
	if err != nil {
		return err
	}
	err = wal.WaterMark()
	if err != nil {
		return err
	}
	return nil
}
//Default handler 
func DefaultHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Unknown command: %s\n", r.URL.Path)
}