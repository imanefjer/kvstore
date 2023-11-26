package main

import (
	"encoding/json"
	"fmt"
	"net/http"
)
var (
	max = 10
)
func GetHandler(w http.ResponseWriter, r *http.Request, tree *Tree, sst *SStables) {
	key := r.URL.Query().Get("key")

	if key == "" {
		fmt.Println("key parameter is missing")
		http.Error(w, "key parameter is missing", http.StatusBadRequest)
		return
	}
	key1 := []byte(key)
	value, exist := tree.Get(key1)

	if !exist {
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

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
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
	err = wal.AppendCommand(&entry)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fmt.Println("Sets key: ", string(key), " value = ", string(value))
	fmt.Println(tree.Len())

	//todo write a function to do this 
	if tree.Len() == max {
		err = sst.Flush(tree)
		tree = &Tree{}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err = wal.WaterMark()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if (sst.numOfSStable == maxFiles){
			err = sst.Compact()
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
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
	err := tree.Del(key1)
	if err != nil {
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
	if tree.Len() == max {
		err = sst.Flush(tree)
		tree = &Tree{}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		err = wal.WaterMark()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if (sst.numOfSStable == maxFiles){
			err = sst.Compact()
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}

	}
}
