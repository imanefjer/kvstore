package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"strings"
)

// TODO  make it binary
// TODO
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

	
	// ErrClosed is returned when an operation cannot be completed because
	// the wal is closed.
	ErrClosed = errors.New("wal closed")
)

type Wal struct {
	file      io.ReadWriteSeeker
	name 		string
	mu        sync.Mutex //

}

func (w *Wal) begin() error {
	_, err := w.file.Seek(0, io.SeekStart)
	return err
}
//encodes the int as slice of bytes 
func encodeInt(x int)[]byte{
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], uint32(x))
	return encoded[:]
}
//decodes the slice of bytes as an int
func decodeInt(encoded []byte) int {
	return int(binary.BigEndian.Uint32(encoded))
}
func encodeVersion(v int)[]byte {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], uint16(v))
	return encoded[:]
}
func decodeVersion(encoded []byte) int {
	return int(binary.BigEndian.Uint16(encoded))
}
//appendCommand write to the wal the command that has been executed it first 
//stores the command (0 if Set and 1 if Del ) then the length of the key then the key
//then the length of the value 
func (w *Wal) AppendCommand(e *Entry) error {
	if w == nil {
		return ErrClosed
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
	len:= keyLen + valueLen + 12
	entry := make([]byte, len)
	copy(entry[0:4], command)
	copy(entry[4:8], keyLenn)
	copy(entry[8:keyLen+8], key)
	copy(entry[keyLen+8:keyLen+12], valueLenn)
	copy(entry[keyLen+12:keyLen+valueLen+12], value)	
	if _, err := w.file.Write(entry); err != nil {
		return err
	}
	return nil
}

// creating a ne wal
func NewWal(f io.ReadWriteSeeker, name string) *Wal {
	return &Wal{
		file:      f,
		name: 	   name,
	}
}


// After each flush to the disk, instead of creating a new WAL, we choose to delete the content of the WAL
// using truncate, which reduces the size of the file, we should check if it's available for the WAL.
func (w *Wal) WaterMark() error {
	_, err := w.file.Seek(0 ,io.SeekEnd)
	if err != nil {
		return err
	}
	watermark := []byte("WATERMARK")
	if _, err := w.file.Write(watermark); err != nil {
		return err
	}
	return nil
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
	var entries []*Entry
	err := w.begin()
	if err != nil {
		return nil, err
	}
	//find the last watermark
	lastWatermarkPos, err := w.findLastWatermarkPosition()

	if lastWatermarkPos >= 0 {
		if err != nil {
			return nil, err
		}
		// Seek to the position after the last watermark
		_, err = w.file.Seek(lastWatermarkPos+int64(watermarkSize), io.SeekStart)
		if err != nil {
			return nil , err
		}
	}

	for {
		// Read command
		var encodedCommandByte [4]byte
		if _, err:= w.file.Read(encodedCommandByte[:]); err == io.EOF{
			break
		}else if err != nil{
			return nil, err
		}
		commandByte := decodeInt(encodedCommandByte[:])
		command := Cmd(commandByte)
		// Read key length
		var encodedKeyLen [4]byte
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
		var encodedValueLength [4]byte
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
			Key:     key,
			Value:   value,
		}

		entries = append(entries, e)
	}

	return entries, nil
}
func (w *Wal)findLastWatermarkPosition() (int64, error) {
	var pos int64 = -1

	buffer := make([]byte, watermarkSize)

	// Get the size of the file
	fileInfo, err := os.Stat(w.name)
	if err != nil {
		return pos, err
	}
	fileSize := fileInfo.Size()

	// Start reading from the end of the file
	for offset := int64(0); offset <= fileSize-watermarkSize; offset++ {
		_, err := w.file.Seek(fileSize-offset-watermarkSize, io.SeekStart)
		if err != nil {
			return 0, err
		}
		_, err = w.file.Read(buffer)
		if err != nil {
			return pos, err
		}

		// Check if the current buffer contains the watermark
		if strings.EqualFold(string(buffer), "WATERMARK") { // Change "mark" to your actual watermark
			pos = fileSize - offset - watermarkSize
			break
		}
	}

	return pos, nil
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
