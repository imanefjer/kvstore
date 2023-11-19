package main

import (
	"fmt"
	"net/http"
	"os"
)

type Cmd int

// // func Gets(key []byte) {
// // 	value := tree.get(key)
// // 	fmt.Println("Gets key: ", string(value))
// // }
// func Sets(key, value []byte) {
// 	fmt.Println("Sets key: ", string(key), " value = ", string(value))

// }
// func Dels(key []byte) {
// 	fmt.Println("dels key: ", string(key))

// }

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
	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		GetHandler(w, r, &tree, wal)

	})
	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		SetHandler(w, r, &tree, wal)

	})
	http.HandleFunc("/del", func(w http.ResponseWriter, r *http.Request) {
		DelHandler(w, r, &tree, wal)
	})
	http.ListenAndServe(":8084", nil)

}
