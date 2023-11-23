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
//TODO make sure that writing is good and then implemnts the searching and check if wz should store the largest key or the length ot the largest key same for smallest 
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
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// Directory does not exist, create it
		err := os.Mkdir(path, 0755) // 0755 is the permission mode, you can adjust it as needed
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		// Some other error occurred
		return nil, err
	} 
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
func (node *Node) format() []byte {
	var marker []byte
	if node.marker {
		marker = encodeInt(1)
	} else {
		marker = encodeInt(0)
	}
	len1 := len(node.Key)
	len2 := len(node.Value)
	keyLen := encodeInt(len1)
	valueLen := encodeInt(len2)
	key := []byte(node.Key)
	value := []byte(node.Value)
	res := make([]byte, len1+len2+24)
	copy(res[0:8], marker)
	copy(res[8:16], keyLen)
	copy(res[16:len1+16], key)
	copy(res[len1+16:len1+24], valueLen)
	copy(res[len1+24:len1+len2+24], value)
	return res
}

// when we flush to disk we create a new sstable that contains the content of the tree
func (s *SStables) Flush(tree *Tree, wal *Wal) error {
	// s.mu.Lock()
	// defer s.mu.Unlock()
	//create a new sstable
	path := fmt.Sprintf(s.path+"/"+s.Name())
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return err
	}
	//write the magic number
	magicNumber := encodeInt(0)
	if _, err := file.Write(magicNumber); err != nil {
		return err
	}
	//write the entry count
	entryCount := encodeInt(tree.Len())
	if _, err := file.Write(entryCount); err != nil {
		return err
	}
	//write the smallest key
	minKey := encodeInt(len(tree.Min()))
	if _, err := file.Write(minKey); err != nil {
		return err
	}
	//write the largest key
	maxKey := encodeInt(len((tree.Max())))
	if _, err := file.Write(maxKey); err != nil {
		return err
	}
	//write the version
	version := encodeInt(1)
	if _, err := file.Write(version); err != nil {
		return err
	}

	for it := tree.Iterator(); it.HasNext(); {
		currNode, err := it.Next()
		if err != nil {
			return err
		}
		nodeFormat := currNode.format()
		if _, err := file.Write(nodeFormat); err != nil {
			return fmt.Errorf("failed to write to disk table %d: %w", s.numOfSStable, err)
		}
	}
	checksum := crc32.ChecksumIEEE([]byte(file.Name()))
	checksumBytes := encodeInt(int(checksum))
	if _, err := file.Write(checksumBytes); err != nil {
		return err
	}
	//close the file
	if err := file.Close(); err != nil {
		return err
	}
	s.numOfSStable++
	return nil
}

// to name the sstable
func (s *SStables) Name() string {
	if s == nil {
		return "Invalid SStables (nil)"
	}
	return fmt.Sprintf("file%d.sst", s.numOfSStable)
}
