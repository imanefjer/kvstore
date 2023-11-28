package main

import (
	"bytes"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"strings"
)
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

}

func (w *Wal) begin() error {
	_, err := w.file.Seek(0, io.SeekStart)
	return err
}

//appendCommand write to the wal the command that has been executed it first 
//stores the command (0 if Set and 1 if Del ) then the length of the key then the key
//then the length of the value 
func (w *Wal) AppendCommand(e *Entry) error {
	if w == nil {
		return ErrClosed
	}
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
		command = encodeNum(0)
	} else {
		command = encodeNum(1)
	}
	len:= keyLen + valueLen + 10
	entry := make([]byte, len)
	copy(entry[0:2], command)
	copy(entry[2:6], keyLenn)
	copy(entry[6:keyLen+6], key)
	copy(entry[keyLen+6:keyLen+10], valueLenn)
	copy(entry[keyLen+10:keyLen+valueLen+10], value)	
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
		var encodedCommandByte [2]byte
		if _, err:= w.file.Read(encodedCommandByte[:]); err == io.EOF{
			break
		}else if err != nil{
			return nil, err
		}
		commandByte := decodeNum(encodedCommandByte[:])
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
//we search for the last WaterMark written in the wal 
// A potential issue arises if someone uses "WATERMARK" as a value or key, as this could lead to an incorrect position detection.

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
