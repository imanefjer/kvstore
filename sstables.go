package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
)
var(
	//ErrKeynotfound is returned when the lod is corrupt
	ErrKeynotfound = errors.New("key not found")
)
type SStable struct {
	file        io.ReadWriteSeeker
	magicNumber [4]byte
	smallestKey [4]byte
	largestKey [4]byte
	entryCount [4]byte
	version [2]byte
	checksum [4]byte 
}
type SStables struct{
	sstables []SStable
	path string //path to the sstable directory
	corrupt bool
	mu sync.RWMutex
	numOfSStable int
}

func Open(path string)(*SStables, error){
	path, err  := filepath.Abs(path)
	if err != nil {
		return nil , err
	}
	sstables := &SStables{
		path: path,
	}
	if err := os.MkdirAll(path, 0750); err != nil{
		return nil, err
	}
	if err := sstables.load(); err != nil{
		return nil, err
	}
	return sstables, nil
}

func (s *SStables) load() error{
	s.mu.Lock()
	defer s.mu.Unlock()
	dir, err := os.Open(s.path)
	if err != nil{
		return err
	}
	defer dir.Close()
	files, err := dir.Readdir(-1)
	if err != nil{
		return err
	}
	for _, file := range files{
		if file.IsDir(){
			continue
		}
		// if err := s.open(file.Name()); err != nil{
		// 	return err
		// }
	}
	return nil
}

func (s *SStables)Get(key []byte)([]byte , error)

func (s *SStables)Flush(tree Tree)(error)

func (s *SStable)Name()(string , error)