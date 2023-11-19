package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	//ErrKeynotfound is returned when the lod is corrupt
	ErrKeynotfound = errors.New("key not found")
)

type SStable struct {
	file        io.ReadWriteSeeker
	magicNumber [4]byte
	smallestKey [4]byte
	largestKey  [4]byte
	entryCount  [4]byte
	version     [2]byte
	checksum    [4]byte
}
type SStables struct {
	sstables     []*SStable
	path         string //path to the sstable directory
	mu           sync.RWMutex
	numOfSStable int
}

// func Open(path string)(*SStables, error){
// 	path, err  := filepath.Abs(path)
// 	if err != nil {
// 		return nil , err
// 	}
// 	sstables := &SStables{
// 		path: path,
// 	}
// 	if err := os.MkdirAll(path, 0750); err != nil{
// 		return nil, err
// 	}
// 	if err := sstables.load(); err != nil{
// 		return nil, err
// 	}
// 	return sstables, nil
// }

//	func (s *SStables) load() error{
//		s.mu.Lock()
//		defer s.mu.Unlock()
//		dir, err := os.Open(s.path)
//		if err != nil{
//			return err
//		}
//		defer dir.Close()
//		files, err := dir.Readdir(-1)
//		if err != nil{
//			return err
//		}
//		for _, file := range files{
//			if file.IsDir(){
//				continue
//			}
//			// if err := s.open(file.Name()); err != nil{
//			// 	return err
//			// }
//		}
//		return nil
//	}
func NewSST(path string) (*SStables, error) {
	// Open the directory
	dir, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer dir.Close()

	// Read the directory contents
	files, err := dir.Readdir(-1)
	if err != nil {
		return nil, err
	}

	// Count the number of files
	numOfSst := 0
	for _, fileInfo := range files {
		if !fileInfo.IsDir() {
			numOfSst++
		}
	}
	if numOfSst == 0 {
		return &SStables{
			path:         path,
			numOfSStable: 0,
		}, nil
	} else {
		sstabless, err := loadSStable(path)
		if err != nil {
			return nil, err
		}
		return &SStables{
			path:         path,
			numOfSStable: numOfSst,
			sstables:     sstabless,
		}, nil
	}
}
func loadSStable(path string) ([]*SStable, error) {
	var sstables []*SStable
	dir, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	files, err := dir.Readdir(-1)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		filePath := filepath.Join(path, file.Name())
		name, err := filepath.Abs(filePath)
		sstable, err := openSStable(name)
		if err != nil {
			return nil, err
		}
		sstables = append(sstables, sstable)
	}
	return sstables, nil
}
func openSStable(path string) (*SStable, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	//check if the file is good no one changed

	//calculate teh checksum of the file
	var content bytes.Buffer

	_, err = io.Copy(&content, file)

	if err != nil {
		return nil, err
	}

	checksum := crc32.ChecksumIEEE(content.Bytes())
	//read the checksum in the end of the file
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	//read magic number
	var magicNumber [4]byte
	if _, err := file.Read(magicNumber[:]); err != nil {
		return nil, err
	}
	if magicNumber != [4]byte{0x00, 0x00, 0x00, 0x00} {
		return nil, errors.New("corrupt sstable")
	}
	//read the entry count
	var entryCount [4]byte
	if _, err := file.Read(entryCount[:]); err != nil {
		return nil, err
	}
	//read the smallest key
	var smallestKey [4]byte
	if _, err := file.Read(smallestKey[:]); err != nil {
		return nil, err
	}
	//read the largest key
	var largestKey [4]byte
	if _, err := file.Read(largestKey[:]); err != nil {
		return nil, err
	}
	//read the version
	var version [2]byte
	if _, err := file.Read(version[:]); err != nil {
		return nil, err
	}

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	offset := fileInfo.Size() - 4
	_, err = file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	buffer := make([]byte, 4)
	_, err = file.Read(buffer)
	if err != nil {
		return nil, err
	}
	var fileChecksum [4]byte
	if _, err := file.Read(fileChecksum[:]); err != nil {
		return nil, err
	}
	checksumUint32 := binary.BigEndian.Uint32(fileChecksum[:])

	if checksum != checksumUint32 {
		return nil, errors.New("corrupt sstable")
	}

	sstable := &SStable{
		magicNumber: magicNumber,
		smallestKey: smallestKey,
		largestKey:  largestKey,
		entryCount:  entryCount,
		version:     version,
		checksum:    fileChecksum,
		file:        file,
	}

	return sstable, nil
}

// to search in the sstables from the newest to the oldest in time for a specific key
func (s *SStables) Get(key []byte) ([]byte, error) {
	return nil, nil
}

// when we flush to disk we create a new sstable that contains the content of the tree
func (s *SStables) Flush(tree *Tree, wal *Wal) error {
	// add abyte fo delete and set from the wal
	s.mu.Lock()
	defer s.mu.Unlock()
	s.numOfSStable++
	//create a new sstable
	file, err := os.OpenFile(s.Name(), os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return err
	}
	defer file.Close()
	//write the magic number
	if _, err := file.Write([]byte{0x00, 0x00, 0x00, 0x00}); err != nil {
		return err
	}
	//write the entry count
	entryCount := make([]byte, 4)
	binary.BigEndian.PutUint32(entryCount, uint32(tree.Len()))
	if _, err := file.Write(entryCount); err != nil {
		return err
	}
	//write the smallest key
	smallestKey := make([]byte, 4)
	minKey := binary.BigEndian.Uint32(tree.Min())
	binary.BigEndian.PutUint32(smallestKey, minKey)
	if _, err := file.Write(smallestKey); err != nil {
		return err
	}
	//write the largest key
	largestKey := make([]byte, 4)
	maxKey := binary.BigEndian.Uint32(tree.Max())
	binary.BigEndian.PutUint32(largestKey, maxKey)
	if _, err := file.Write(largestKey); err != nil {
		return err
	}
	//write the version
	version := make([]byte, 2)
	binary.BigEndian.PutUint16(version, uint16(1))
	if _, err := file.Write(version); err != nil {
		return err
	}
	//write the entries
	tree.Ascend(func(key []byte, value []byte) bool {
		//write the key length
		keyLen := make([]byte, 1)
		keyLen[0] = byte(len(key))
		if _, err := file.Write(keyLen); err != nil {
			return false
		}
		//write the value length
		valueLen := make([]byte, 1)
		valueLen[0] = byte(len(value))
		if _, err := file.Write(valueLen); err != nil {
			return false
		}
		//write the key
		if _, err := file.Write(key); err != nil {
			return false
		}
		//write the value
		if _, err := file.Write(value); err != nil {
			return false
		}
		return true
	})
	//write the checksum
	checksum := crc32.ChecksumIEEE([]byte(file.Name()))
	checksumBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(checksumBytes, checksum)
	if _, err := file.Write(checksumBytes); err != nil {
		return err
	}
	s.numOfSStable++
	return nil
}

// to name the sstable
func (s *SStables) Name() string {
	return fmt.Sprintf("file%d.sst", s.numOfSStable)

}
