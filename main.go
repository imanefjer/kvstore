package main

import (
	"fmt"
	"net/http"
	"os"
)

func main() {
	f, err := os.OpenFile("wal.log", os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()
	wal := NewWal(f)
	wal.checksum, err = wal.CalculateCheckSum()
	if err != nil {
		fmt.Println(err)
	}
	tree := Tree{}
	// value := "hjkh"
	// tree.Set([]byte("hey"), []byte("bye"))
	// tree.Set([]byte("hey1"), []byte("bye1"))
	// tree.Set([]byte("hey2"), []byte("bye2"))
	// value, exists := tree.Get([]byte("hey1"))
	// if exists {
	// 	fmt.Println("Gets key: ", string(value))
	// } else{
	// 	fmt.Println("Key not found")
	// }
	// Print(&tree)
	// tree.Del([]byte("hey2"))
	// tree.Del([]byte("hey1"))
	// tree.Del([]byte("hey"))
	// fmt.Println("///////////////////////////////\\\"")
	// Print(&tree)
	// value, exists = tree.Get([]byte("hey1"))
	// if exists {
	// 	fmt.Println("Gets key: ", string(value))
	// } else{
	// 	fmt.Println("Key not found")
	// }
	//In case the program exits and some elements in the tree were not flushed to disk
	err = Recover(wal, &tree)
	if err != nil {
		fmt.Println(err)
	}
	sst, err := NewSST("sstFiles")
	if err != nil {
		fmt.Println(err)
	}
	err = sst.Flush(&tree, wal)
	if err != nil{
		fmt.Println(err)
	}
	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		GetHandler(w, r, &tree)

	})
	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		SetHandler(w, r, &tree, wal)

	})
	http.HandleFunc("/del", func(w http.ResponseWriter, r *http.Request) {
		DelHandler(w, r, &tree, wal)
	})
	http.ListenAndServe(":8084", nil)

}
