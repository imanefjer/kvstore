package main

import (
	"bytes"
	// "context"
	"errors"
	"hash/crc32"
	"io"
	"sync"
)

type Entry struct {
	Key     []byte
	Value   []byte
	Command []byte
}

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

// To write in the WAL: each entry has a fixed size of 100. We store the length of the key and the length
// of the value to make the Read function easier. Then, we store the command, key, and value. We ensure
// that the WAL was not corrupted by checking the checksum, and we update it after writing to the file.
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
	if e == nil {
		return errors.New("nil entry")
	}
	if e.Command == nil {
		return errors.New("nil command")
	}
	if e.Key == nil {
		return errors.New("nil key")
	}
	keyLen, valueLen := len(e.Key), len(e.Value)
	entry := make([]byte, w.entrySize)
	entry[0] = byte(keyLen)
	entry[1] = byte(valueLen)
	copy(entry[2:5], e.Command)
	copy(entry[5:keyLen+5], e.Key)
	copy(entry[keyLen+5:keyLen+valueLen+5], e.Value)
	_, err = w.file.Write(entry)
	if err != nil {
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

// We assume that the key length and the value length will not exceed 255.
// We read the WAL and return all the entries that we had in this WAL.

func (w *Wal) Read() ([]*Entry, error) {
	if w == nil {
		return nil, ErrClosed
	}
	
	checksum, err := w.CalculateCheckSum()
	if err != nil {
		return nil, err
	}
	if checksum != w.checksum {
		return nil, ErrCorrupt
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	var entries []*Entry
	err = w.begin()
	if err != nil {
		return nil, err
	}
	for {
		entry := make([]byte, w.entrySize)
		n, err := w.file.Read(entry)
		if err == io.EOF {
			// End of file reached
			break
		} else if err != nil {
			return nil, err
		}
		if n != w.entrySize {
			return nil, errors.New("unexpected entry size")
		}
		keyLen, valueLen := entry[0], entry[1]

		e := &Entry{
			Command: entry[2:5],
			Key:     entry[5 : 5+keyLen],
			Value:   entry[5+keyLen : 5+keyLen+valueLen],
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
	w.mu.Lock()
	defer w.mu.Unlock()

	entries, err := w.Read()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		switch string(entry.Command) {
		case "SET":
			t.Set(entry.Key, entry.Value)
		case "DEL":
			t.Del(entry.Key)
		}
	}
	return nil
}
