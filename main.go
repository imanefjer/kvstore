package main

import (
	"fmt"
	"net/http"
	"os"
)

type Cmd int

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
	//In case the program exits and some elements in the tree were not flushed to disk
	err = Recover(wal, &tree)
	if err != nil {
		fmt.Println(err)
	}

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
