package main

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)
func TestSStablesFlush(t *testing.T) {
	// Create a temporary directory for testing
	// tmpDir, err := ioutil.TempDir("", "testSStables")
	// if err != nil {
	// 	t.Fatal("Failed to create temporary directory:", err)
	// }
	// defer os.RemoveAll(tmpDir)

	// Test data
	tree := &Tree{}	
	key := generateRandomBytes(8) // Change the size based on your key size
	value := generateRandomBytes(16) // Change the size based on your value size
	tree.Set(key, value)
	

	// Create an SStables instance
	sstables,err := NewSST("testSStables")
	if err != nil {
		t.Fatal("Failed to create SStables instance:", err)
	}
	// Test the Flush method
	err = sstables.Flush(tree)
	// Check the results
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Verify that the SSTable sstableFile has been created
	if err != nil {
		t.Fatalf("Failed to read directory: %v", err)
	}

	sstableFiles, err := ioutil.ReadDir("testSStables")
	if len(sstableFiles) != 1 {
		t.Fatalf("Expected one SSTable sstableFile, but found %d", len(sstableFiles))
	}
	path := fmt.Sprintf("testSStables" + "/" + sstableFiles[0].Name())

	// Read the contents of the SSTable sstableFile and verify it matches what we expect
	sstableFile, err := os.Open(path)
	if err != nil {
		t.Fatalf("Failed to open SSTable sstableFile: %v", err)
	}
	defer sstableFile.Close()
	var magicNumber [4]byte
	if _, err := sstableFile.Read(magicNumber[:]); err != nil {
		t.Fatalf("Failed to read SSTable File")
	}
	if decodeInt(magicNumber[:]) != 1234 {
		t.Fatalf("SSTable File corrupt")
	}
	var entryCount [4]byte
	if _, err := sstableFile.Read(entryCount[:]); err != nil {
		t.Fatalf("Failed to read SSTable File")
	}
	//read the smallest key
	var smallestKeyL [4]byte
	if _, err := sstableFile.Read(smallestKeyL[:]); err != nil {
		t.Fatalf("Failed to read SSTable File")
	}
	smallestKeyLen := decodeInt(smallestKeyL[:])
	smallestKey := make([]byte, smallestKeyLen)
	if _, err = sstableFile.Read(smallestKey); err != nil {
		t.Fatalf("Failed to read SSTable File")
	}
	//read the largest key
	var largestKeyL [4]byte
	if _, err := sstableFile.Read(largestKeyL[:]); err != nil {
		t.Fatalf("Failed to read SSTable File")
	}
	largestKeyLen := decodeInt(largestKeyL[:])
	largestKey := make([]byte, largestKeyLen)
	if _, err = sstableFile.Read(largestKey); err != nil {
		t.Fatalf("Failed to read SSTable File")
	}
	//read the version
	var versionEncoded [2]byte
	if _, err := sstableFile.Read(versionEncoded[:]); err != nil {
		t.Fatalf("Failed to read SSTable File")
	}

	var marker [2]byte
	_, err = sstableFile.Read(marker[:])
	if err != nil {
		t.Fatalf("Failed to read SSTable File")
	}
	var keyLen [4]byte
	if _, err := sstableFile.Read(keyLen[:]); err != nil {
		t.Fatalf("Failed to read SSTable File")
	}
	keyLenInt := decodeInt(keyLen[:])
	key1 := make([]byte, keyLenInt)
	if _, err := sstableFile.Read(key1); err != nil {
		t.Fatalf("Failed to read SSTable File")
	}
	var valueLen [4]byte
	if _, err := sstableFile.Read(valueLen[:]); err != nil {
		t.Fatalf("Failed to read SSTable File")
	}
	valueLenInt := decodeInt(valueLen[:])
	value1 := make([]byte, valueLenInt)

	if _, err := sstableFile.Read(value1); err != nil {
		t.Fatalf("Failed to read SSTable File")
	}
	if (!bytes.Equal(key, key1) || !bytes.Equal(value1, value)){
		t.Fatalf("incorrect values")
	}
}

// Helper function to generate random bytes
func generateRandomBytes(size int) []byte {
	b := make([]byte, size)
	_, err := rand.Read(b)
	if err != nil {
		panic("Failed to generate random bytes")
	}
	return b
}