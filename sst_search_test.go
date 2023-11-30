package main

import (
	"bytes"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"testing"
)
func TestSStableSearch(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := ioutil.TempFile("", "testSStable")
	if err != nil {
		t.Fatal("Failed to create temporary file:", err)
	}
	defer os.Remove(tmpFile.Name())

	// Test data
	testKey := []byte("testKey")
	testValue := []byte("testValue")

	// Create an SStable instance with known values
	sstable := &SStable{
		magicNumber: [4]byte{1, 2, 3, 4},
		smallestKey: []byte("aaa"),
		largestKey:  []byte("zzz"),
		entryCount:  1,
		version:     1,
		name:        "testSStable.sst",
	}

	// Open the file and write the SStable instance to it
	file, err := os.OpenFile(sstable.name, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		t.Fatal("Failed to open file for writing:", err)
	}
	defer file.Close()

	// Write the SStable instance to the file
	if err := writeSStableToDisk(file, sstable, testKey, testValue); err != nil {
		t.Fatal("Failed to write SStable to file:", err)
	}

	// Test the search method
	value, err := sstable.search(testKey)

	// Check the results
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if !bytes.Equal(value, testValue) {
		t.Fatalf("Expected value %v, but got %v", testValue, value)
	}
}

// Helper function to write SStable instance to file
func writeSStableToDisk(file *os.File, sstable *SStable, key, value []byte) error {
	// Write the magic number, entry count, smallest key, largest key, version, key, and value
	if _, err := file.Write(sstable.magicNumber[:]); err != nil {
		return err
	}
	if _, err := file.Write(encodeInt(sstable.entryCount)); err != nil {
		return err
	}
	if _, err := file.Write(encodeInt(len(sstable.smallestKey))); err != nil {
		return err
	}
	if _, err := file.Write(sstable.smallestKey); err != nil {
		return err
	}
	if _, err := file.Write(encodeInt(len(sstable.largestKey))); err != nil {
		return err
	}
	if _, err := file.Write(sstable.largestKey); err != nil {
		return err
	}
	if _, err := file.Write(encodeNum(sstable.version)); err != nil {
		return err
	}
	if _, err := file.Write(encodeNum(1)); err != nil { // 1 represents a valid, non-deleted entry
		return err
	}
	if _, err := file.Write(encodeInt(len(key))); err != nil {
		return err
	}
	if _, err := file.Write(key); err != nil {
		return err
	}
	if _, err := file.Write(encodeInt(len(value))); err != nil {
		return err
	}
	if _, err := file.Write(value); err != nil {
		return err
	}
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return  err
	}
	var content bytes.Buffer

	_, err = io.Copy(&content, file)

	if err != nil {
		return err
	}
	checksum := crc32.ChecksumIEEE(content.Bytes())
	checksumBytes := encodeInt(int(checksum))
	if _, err := file.Write(checksumBytes); err != nil {
		return  err
	}
	sstable.checksum = int(checksum)
	return nil
}
