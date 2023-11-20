package main

import (
	"bytes"
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

	keyLen, valueLen := len(e.Key), len(e.Value)
	key := make([]byte, keyLen)
	value := make([]byte, valueLen)
	keyLenn := make([]byte, 1)
	valueLenn := make([]byte, 1)
	keyLenn[0] = byte(keyLen)
	valueLenn[0] = byte(valueLen)
	copy(key[:], e.Key)
	copy(value[:], e.Value)
	command := make([]byte, 1)

	if e.Command == Set {
		command[0] = byte(0)
	} else {
		command[0] = byte(1)
	}
	len:= keyLen + valueLen + 1 + 1 + 1
	entry := make([]byte, len)
	//command
	// binary.BigEndian.PutUint32(command,uint32(byte(e.Command)))
	// if _, err := w.file.Write(command); err != nil {
	// 	return err
	// }
	copy(entry[0:1], command)
	//key length
	// binary.BigEndian.PutUint32(keyLenn, uint32(keyLen))
	// if _, err := w.file.Write(keyLenn); err != nil {
	// 	return err
	// }
	copy(entry[1:2], keyLenn)
	//key
	// keyUint32 := binary.BigEndian.Uint32(e.Key)
	// binary.BigEndian.PutUint32(key, keyUint32)
	// if _, err := w.file.Write(key); err != nil {
	// 	return err
	// }

	copy(entry[2:keyLen+2], key)
	//value length
	// binary.BigEndian.PutUint32(valueLenn, uint32(valueLen))
	// if _, err := w.file.Write(valueLenn); err != nil {
	// 	return err
	// }
	copy(entry[keyLen+2:keyLen+3], valueLenn)
	//value
	// valueUint32 := binary.BigEndian.Uint32(e.Value)
	// binary.BigEndian.PutUint32(key, valueUint32)
	// if _, err := w.file.Write(key); err != nil {
	// 	return err
	// }
	copy(entry[keyLen+3:keyLen+valueLen+3], value)

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
	w.mu.Lock()
	defer w.mu.Unlock()
	checksum, err := w.CalculateCheckSum()
	if err != nil {
		return nil, err
	}
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
		commandByte := make([]byte, 1)
		_, err := w.file.Read(commandByte)
		if err == io.EOF {
			// End of file reached
			break
		} else if err != nil {
			return nil, err
		}
		command := Cmd(commandByte[0])

		// Read key length
		keyLenByte := make([]byte, 1)
		_, err = w.file.Read(keyLenByte)
		if err != nil {
			return nil, err
		}
		keyLen := int(keyLenByte[0])

		// Read key
		key := make([]byte, keyLen)
		_, err = w.file.Read(key)
		if err != nil {
			return nil, err
		}

		// Read value length
		valueLenByte := make([]byte, 1)
		_, err = w.file.Read(valueLenByte)
		if err != nil {
			return nil, err
		}
		valueLen := int(valueLenByte[0])

		// Read value
		value := make([]byte, valueLen)
		_, err = w.file.Read(value)
		if err != nil {
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
