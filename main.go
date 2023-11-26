package main

import (
	"fmt"
	"net/http"
	"os"
)

const (
	watermarkSize = 9 // Change this to the size of your watermark in bytes
)
//Todo make the count to flush to disk 100 and remove all the unnecessary fmt.println 
func main() {
	
	f, err := os.OpenFile("wal.log", os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()
	wal := NewWal(f,"wal.log")
	// wal.checksum, err = wal.CalculateCheckSum()
	if err != nil {
		fmt.Println(err)
	}
	tree := Tree{}
	//In case the program exits and some elements in the tree were not flushed to disk
	err = Recover(wal, &tree)
	if err != nil {
		fmt.Println("tes")
		fmt.Println(err)
	}
	sst, err := NewSST("sstFiles")
	if err != nil {
		fmt.Println(err)
	}
	err = sst.Flush(&tree)
	if err != nil{
		fmt.Println(err)
	}
	err = sst.Compact()
	if err != nil{
		fmt.Println(err)
	}
	fmt.Println(sst.numOfSStable)
	
	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		GetHandler(w, r, &tree, sst)
	})

	http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		SetHandler(w, r, &tree, wal, sst)
	})

	http.HandleFunc("/del", func(w http.ResponseWriter, r *http.Request) {
		DelHandler(w, r, &tree, wal, sst)
	})

	http.ListenAndServe(":8084", nil)

}



