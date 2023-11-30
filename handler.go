package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)

var (
	max = 500
)

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
// To handle the 'get' operation, the process begins by checking if the key is not empty.
// Subsequently, the database's tree is queried to determine if the key exists.
// If the key is found in the tree, the operation concludes, and the corresponding value is returned.
// If the key is not present in the tree, a search in the SSTables is initiated using the sst.search() function.
// If the key is found in the SSTables, the operation concludes, and the corresponding value is returned.
// If the key is not found in the SSTables, the operation concludes, and an error(key not found) is returned.
func GetHandler(w http.ResponseWriter, r *http.Request, db *DB) {
	key := r.URL.Query().Get("key")

	if key == "" {
		fmt.Println("key parameter is missing")
		http.Error(w, "key parameter is missing", http.StatusBadRequest)
		return
	}
	key1 := []byte(key)
	//check if the value is in the tree if not we search in the sstfiles
	value, err := db.tree.Get(key1)
	if err == ErrDeleted {
		fmt.Println("key not found")
		http.Error(w, "key not found", http.StatusBadRequest)
		return
	}
	if err == ErrKeynotfound {
		// search in sstfiles
		value, err := db.sst.Search(key1)
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
		//print in the page
		fmt.Fprintf(w, "key: %s, value: %s \n", string(key), string(value))
		return
	}
	fmt.Println("Gets key:  ", string(value))
		//print in the page
		fmt.Fprintf(w, "key: %s, value: %s \n", string(key), string(value))
	fmt.Println("Gets key: ", string(value))
}
// To handle the 'set' operation, the process initiates by extracting the key and value from the JSON format and check if 
//they are not empty. If they are not empty we set the value in the tree and add the command to the wal.
//If the tree has reached the maximum length it needs to be flushed to disk
func SetHandler(w http.ResponseWriter, r *http.Request, db *DB) {
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
	err1 := db.tree.Set(key1, value1)
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
	err = db.wal.AppendCommand(&entry)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fmt.Println("Sets key: ", string(key), " value = ", string(value))
	fmt.Fprintf(w, "Sets key: %s, value: %s \n", string(key), string(value))

	//if the tree has reached the maximum length it needs to be flushed to disk
	if db.tree.Len() == max {
		if err := FlushToDisk(db); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
}
// To handle the 'del' operation, we first check if the given key is not null. 
// If the key is found in the tree, the corresponding node is marked as deleted by changing its marker to 0.
// If the key is not present in the tree, we search for it in the SSTables. If found, 
// a deleted node (marked with 0) is created in the tree, and the deletion command is added to the WAL.
// Additionally, if the tree has reached its maximum length, it needs to be flushed to disk to maintain efficiency.
func DelHandler(w http.ResponseWriter, r *http.Request, db *DB) {
	key := r.URL.Query().Get("key")

	if key == "" {
		fmt.Println("key parameter is missing")
		http.Error(w, "key parameter is missing", http.StatusBadRequest)
		return
	}
	key1 := []byte(key)
	// if the key is in the tree it will be deleted if not we search it the sstfiles
	//if the key is there then we add it to the tree as a deleted one (marker = false)
	err := db.tree.Del(key1)
	if err == ErrKeynotfound {
		//the key is not in the tree we should search if it in the sstfiles
		value, err := db.sst.Search(key1)
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
		err1 := db.wal.AppendCommand(&entry)
		if err1 != nil {
			http.Error(w, err1.Error(), http.StatusBadRequest)
			return
		}
		//add it to the tree but the marker will be false
		err = db.tree.SetDeletedKey(key1, value)
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
	err1 := db.wal.AppendCommand(&entry)
	if err1 != nil {
		http.Error(w, err1.Error(), http.StatusBadRequest)
		return
	}
	fmt.Println("the deleted key: ", string(key))
	fmt.Fprintf(w, "the deleted key: %s ", string(key))
	//if the tree has reached the maximum length it needs to be flushed to disk

	if db.tree.Len() == max {
		if err := FlushToDisk(db); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

}

// The FlushToDisk function flushes the tree to disk, reinitializes the tree, and add
// the watermark in the wal
func FlushToDisk(db *DB) error {
	err := db.sst.Flush(db.tree)
	if err != nil {
		return err
	}
	err = db.tree.Reinitialize()
	if err != nil {
		return err
	}
	err = db.wal.WaterMark()
	if err != nil {
		return err
	}
	return nil
}

// Default handler
func DefaultHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Unknown command: %s\n", r.URL.Path)
}
