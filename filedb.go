package main

import (
	"bytes"
	"errors"
	"io"
)

type fileDB struct {
	file io.ReadWriteSeeker

	Term  byte
	Bsize int
}

func (fl *fileDB) begin() error {
	_, err := fl.file.Seek(0, io.SeekStart)
	return err
}

func (fl *fileDB) findFirst(buf []byte, term byte) int {
	for i, v := range buf {
		if v == term {
			return i
		}
	}
	return -1
}

func (fl *fileDB) findBlock(key []byte) (int, int, []byte, error) {
	if err := fl.begin(); err != nil {
		return -1, -1, nil, err
	}
	buf := make([]byte, fl.Bsize)
	block, writeBlock := 0, -1
	for ; ; block++ {
		if _, err := fl.file.Seek(int64(block*fl.Bsize), io.SeekStart); err != nil {
			return -1, -1, nil, err
		}
		_, err := fl.file.Read(buf)
		if err == io.EOF {
			break
		}

		oin := fl.findFirst(buf, fl.Term)
		if oin < 0 {
			panic("Unexpected: block with no terminator character")
		}

		// When oin is equal to '0', this line is empty, which makes it a good candidate
		// to write new entries.
		if oin == 0 {
			writeBlock = block
			continue
		}

		// This code will only be reached when we have a valid key=value pair in the current block
		parts := bytes.Split(buf[:oin], []byte("="))

		if len(parts) != 2 {
			panic("Unexpected: key value pair malformed")
		}

		if bytes.Equal( /*key*/ parts[0], key) {
			return block, block, parts[1], nil
		}
		// Move on to the next block
	}

	if writeBlock >= 0 {
		return writeBlock, -1, nil, nil
	}
	// Return the latest block to write to
	return block, -1, nil, nil
}

func (fl *fileDB) Set(key, value []byte) error {
	// Find if the key exists
	//     if the key exists, gets the current block number of the key  override the block content with
	//     key=newValue#paddingbytes
	// Otherwise:
	//    If a empty slot exists:
	//     Find an empty line and insert the key=newvalue
	//    Else
	//      Append to the end
	if len(key)+len(value) > fl.Bsize-2 {
		return errors.New("key and value combination length is too long for the block size")
	}
	bl, _, _, err := fl.findBlock(key)
	if err != nil {
		return err
	}

	entry := make([]byte, fl.Bsize)
	keyLen, valueLen := len(key), len(value)

	copy(entry[:keyLen], key)
	entry[keyLen] = '='
	copy(entry[keyLen+1:keyLen+1+valueLen], value)
	entry[keyLen+1+valueLen] = fl.Term

	// Writing padding
	for i := keyLen + valueLen + 2; i < fl.Bsize; i++ {
		entry[i] = '0'
	}
	return fl.Writetoblock(entry, bl)
}

func (fl *fileDB) Get(key []byte) ([]byte, error) {
	if err := fl.begin(); err != nil {
		return nil, err
	}
	_, _, value, err := fl.findBlock(key)
	if err != nil {
		return nil, err
	}
	if value != nil {
		return value, nil
	}
	return nil, errors.New("key not found")
}

func (fl *fileDB) Writetoblock(entry []byte, bl int) error {
	if _, err := fl.file.Seek(int64(bl*fl.Bsize), io.SeekStart); err != nil {
		return err
	}
	if _, err := fl.file.Write(entry); err != nil {
		return err
	}
	return nil
}
func (fl *fileDB) Del(key []byte) ([]byte, error) {
	// Find if the key exists
	//     if the key exists, gets the current block number of the key  override the block content with
	//     #00000000
	// Otherwise:
	// 	  key not found
	if err := fl.begin(); err != nil {
		return nil, err
	}
	_, block, _, err := fl.findBlock(key)
	if err != nil {
		return nil, err
	}
	if block == -1 {
		return nil, errors.New("key not found")
	} else {
		// Delete the key
		entry := make([]byte, fl.Bsize)
		entry[0] = '#'
		for i := 1; i < fl.Bsize; i++ {
			entry[i] = '0'
		}
		if err := fl.Writetoblock(entry, block); err != nil {
			return nil, err
		}
		return key, nil
	}
}

func NewFileDB(f io.ReadWriteSeeker) *fileDB {
	return &fileDB{
		file:  f,
		Bsize: 100,
		Term:  '#',
	}
}
