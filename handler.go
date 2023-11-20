package main

import (
	"fmt"
	"net/http"
)

func GetHandler(w http.ResponseWriter, r *http.Request, tree *Tree) {
	key := r.URL.Query().Get("key")

	if key == "" {
		fmt.Println("key parameter is missing")
		http.Error(w, "key parameter is missing", http.StatusBadRequest)
		return
	}
	key1 := []byte(key)
	value, err := tree.Get(key1)
	
	if !err {
		fmt.Println("key not found")
		http.Error(w, "key not found", http.StatusBadRequest)
		return
	}
	fmt.Println("Gets key: ", string(value))
}

func SetHandler(w http.ResponseWriter, r *http.Request, tree *Tree, wal *Wal) {
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")

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
	err := wal.AppendCommand(&entry)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fmt.Println("Sets key: ", string(key), " value = ", string(value))
}
func DelHandler(w http.ResponseWriter, r *http.Request, tree *Tree, wal *Wal) {
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
}
