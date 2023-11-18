package main

import (
	"fmt"
	"net/http"
)

func GetHandler(w http.ResponseWriter, r *http.Request, tree *Tree) {
	key := r.URL.Query().Get("key")

	if key == "" {
		http.Error(w, "key parameter is missing", http.StatusBadRequest)
		return
	}
	key1 := []byte(key)
	value, err := tree.Get(key1)
	if !err {
		http.Error(w, "key not found", http.StatusBadRequest)
		return
	}
	fmt.Println("Gets key: ", string(value))
}

func SetHandler(w http.ResponseWriter, r *http.Request, tree *Tree) {
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")

	if key == "" || value == "" {
		http.Error(w, "key or value parameter is missing", http.StatusBadRequest)
		return
	}
	key1 := []byte(key)
	value1 := []byte(value)
	tree.Set(key1, value1)
	fmt.Println("Sets key: ", string(key), " value = ", string(value))
}
func DelHandler(w http.ResponseWriter, r *http.Request, tree *Tree)  {
	key := r.URL.Query().Get("key")

	if key == "" {
		http.Error(w, "key parameter is missing", http.StatusBadRequest)	
		return	
	}
	key1 := []byte(key)
	err := tree.Del(key1)
	if err != nil {
		http.Error(w, "key not found", http.StatusBadRequest)
		return
	}
	fmt.Println("the deleted key: ", string(key))
}
