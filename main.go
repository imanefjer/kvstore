package main

import (
	"fmt"
	"net/http"
	"os"
)

const (
	watermarkSize = 9 // Change this to the size of your watermark in bytes
)

// todo readme testing code
// Todo make the count to flush to disk 100 and remove all the unnecessary fmt.println
func main() {

	f, err := os.OpenFile("wal.log", os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()
	wal := NewWal(f, "wal.log")
	//In case the program exits and some elements in the tree were not flushed to disk
	db, err := NewDB(wal)
	if err != nil {
		fmt.Println(err)
	}

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		GetHandler(w, r, db)
	})

	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		SetHandler(w, r, db)
	})

	http.HandleFunc("/del", func(w http.ResponseWriter, r *http.Request) {
		DelHandler(w, r, db)
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		DefaultHandler(w, r)
	})

	http.ListenAndServe(":8084", nil)

}
