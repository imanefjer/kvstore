package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"sync"
)

// TODO  make it binary
type Entry struct {
	Key     []byte
	Value   []byte
	Command Cmd
}

type Cmd int

const (
	Set Cmd = iota
	Del
)

var (

	//ErrCorrupt is returned when the lod is corrupt
	ErrCorrupt = errors.New("wal corrupt")
	// ErrClosed is returned when an operation cannot be completed because
	// the wal is closed.
	ErrClosed = errors.New("wal closed")
)

type Wal struct {
	file      io.ReadWriteSeeker
	entrySize int
	checksum  uint32
	mu        sync.Mutex //

}

func (w *Wal) begin() error {
	_, err := w.file.Seek(0, io.SeekStart)
	return err
}
//encodes the int as slice of bytes 
func encodeInt(x int)[]byte{
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(x))
	return encoded[:]
}
//decodes the slice of bytes as an int
func decodeInt(encoded []byte) int {
	return int(binary.BigEndian.Uint64(encoded))
}

//appendCommand write to the wal the command that has been executed it first 
//stores the command (0 if Set and 1 if Del ) then the length of the key then the key
//then the length of the value and the finally the value and it update the checksum associated to the wal
func (w *Wal) AppendCommand(e *Entry) error {
	if w == nil {
		return ErrClosed
	}
	checksum, err := w.CalculateCheckSum()
	if err != nil {
		return err
	}
	if checksum != w.checksum {
		return ErrCorrupt
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	//TODO try to make this a function to not repeat the same thing 
	if e == nil {
		return errors.New("nil entry")
	}
	if e.Command != Set && e.Command != Del{
		return errors.New("invalid command")
	}
	if e.Key == nil {
		return errors.New("nil key")
	}
	//the value could be nil if the command is del
	if e.Value == nil && e.Command == Set {
		return errors.New("nil value")
	}

	keyLen := len(e.Key)
	valueLen := len(e.Value)
	keyLenn := encodeInt(keyLen)
	valueLenn := encodeInt(valueLen)
	key := []byte(e.Key)
	value := []byte(e.Value)
	var command []byte
	if e.Command == Set {
		command = encodeInt(0)
	} else {
		command = encodeInt(1)
	}
	len:= keyLen + valueLen + 24
	entry := make([]byte, len)
	copy(entry[0:8], command)
	copy(entry[8:16], keyLenn)
	copy(entry[16:keyLen+16], key)
	copy(entry[keyLen+16:keyLen+24], valueLenn)
	copy(entry[keyLen+24:keyLen+valueLen+24], value)	
	if _, err := w.file.Write(entry); err != nil {
		return err
	}
	w.checksum, _ = w.CalculateCheckSum()
	return nil
}

// creating a ne wal
func NewWal(f io.ReadWriteSeeker) *Wal {
	return &Wal{
		file:      f,
		entrySize: 100,
		checksum:  0,
	}
}


// After each flush to the disk, instead of creating a new WAL, we choose to delete the content of the WAL
// using truncate, which reduces the size of the file, we should check if it's available for the WAL.
func (w *Wal) DeleteContent(filePath string) error {
	checksum, err := w.CalculateCheckSum()
	if err != nil {
		return err
	}
	if checksum != w.checksum {
		return ErrCorrupt
	}
	if f, ok := w.file.(interface{ Truncate(size int64) error }); ok {
		// Truncate the file
		err := f.Truncate(0)
		if err != nil {
			return err
		}
		w.checksum = 0
		return nil
	} else {
		return errors.New("truncate method is not available")
	}
}

// Read function will loop and  read firstly the command and encoded if its the EOF then
// it will break if it not it will try and read it and associated to the specific command
// then it will read the key length, the key, the value length and the value
func (w *Wal) Read() ([]*Entry, error) {
	if w == nil {
		return nil, ErrClosed
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	checksum, err := w.CalculateCheckSum()
	if err != nil {
		return nil, err
	}
	//check if the wal was not corrupt
	if checksum != w.checksum {
		return nil, ErrCorrupt
	}
	
	var entries []*Entry
	err = w.begin()
	if err != nil {
		return nil, err
	}
	
	for {
		// Read command
		var encodedCommandByte [8]byte
		if _, err:= w.file.Read(encodedCommandByte[:]); err == io.EOF{
			break
		}else if err != nil{
			return nil, err
		}
		commandByte := decodeInt(encodedCommandByte[:])
		command := Cmd(commandByte)
		// Read key length
		var encodedKeyLen [8]byte
		if _, err:= w.file.Read(encodedKeyLen[:]); err != nil{
			return nil, err
		}
		// fmt.Println("\n")
		keyLen := decodeInt(encodedKeyLen[:])
		// Read key
		key := make([]byte, keyLen)
		if _, err:= w.file.Read(key); err != nil{
			return nil, err
		}
		// Read value length
		var encodedValueLength [8]byte
		if _, err:= w.file.Read(encodedValueLength[:]); err != nil{
			return nil, err
		}
		valueLen := decodeInt(encodedValueLength[:])
		// Read value
		value := make([]byte, valueLen)
		if _, err:= w.file.Read(value); err != nil{
			return nil, err
		}
		e := &Entry{
			Command: command,
			Key:     nil,
			Value:   nil,
		}

		entries = append(entries, e)
	}

	return entries, nil
}

func (w *Wal) CalculateCheckSum() (uint32, error) {
	err := w.begin()
	if err != nil {
		return 0, err
	}
	var content bytes.Buffer

	_, err = io.Copy(&content, w.file)

	if err != nil {
		return 0, err
	}

	checksum := crc32.ChecksumIEEE(content.Bytes())
	return checksum, nil

}

// In case of a crash, we use this function to redo the previous commands that were recorded
// before the crash but weren't uploaded to the SSTables.
func Recover(w *Wal, t *Tree) error {
	checksum, err := w.CalculateCheckSum()
	if err != nil {
		return err
	}
	if checksum != w.checksum {
		return ErrCorrupt
	}

	entries, err := w.Read()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		switch entry.Command {
		case Set:
			t.Set(entry.Key, entry.Value)
		case Del:
			t.Del(entry.Key)
		}
	}
	return nil
}
