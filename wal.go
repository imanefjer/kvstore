package main

import (
	"errors"
	"io"
)

type Entry struct {
	Key     []byte
	Value   []byte
	Command []byte
}

var (
	// ErrNotFound is returned when a key is not found in the tree.
	ErrNotFound = errors.New("key not found")
	//ErrCorrupt is returned when the lod is corrupt
	ErrCorrupt  = errors.New("wal corrupt")
	// ErrClosed is returned when an operation cannot be completed because
	// the wal is closed.
	ErrClosed = errors.New("wal closed")

)
type wal struct{
	file io.ReadWriter
	entrySize int
}

func (w *wal) Write(e *Entry) error {
	if w == nil {
		return ErrClosed
	}
	if e == nil {
		return errors.New("nil entry")
	}
	if e.Key == nil {
		return errors.New("nil key")
	}
	if e.Value == nil {
		return errors.New("nil value")
	}
	if e.Command == nil {
		return errors.New("nil command")
	}
	keyLen, valueLen:= len(e.Key) , len(e.Value)	
	entry := make([]byte, w.entrySize)
	entry[0] = byte(keyLen)
	entry[1] = byte(valueLen)
	copy(entry[2:5], e.Command)
	copy(entry[5:keyLen+5], e.Key)
	copy(entry[keyLen+5:keyLen+valueLen+5], e.Value)
	_,err := w.file.Write(entry)
	if err != nil {
		return err
	}
	return nil
}

func NewWal(f io.ReadWriter)*wal {
	return &wal{
		file: f, 
		entrySize: 100,
	}
}

func (w *wal)DeleteContent(filePath string) error{
	if f, ok := w.file.(interface{ Truncate(size int64) error }); ok {
		// Truncate the file
		err := f.Truncate(0)
		if err != nil {
			return err
		}
		return nil
	} else {
		return errors.New("truncate method is not available")
	}
}
// we assume that the key length and the value length will not succeed 255 
// we read the wal and we return all the entries that we had in the this wal 
func (w *wal) Read() ([]*Entry ,error){
	if w == nil {
        return nil, ErrClosed
    }

    var entries []*Entry
	for {
        entry := make([]byte, w.entrySize)
        _, err := w.file.Read(entry)
        if err == io.EOF {
            // End of file reached
            break
        } else if err != nil {
            return nil, err
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

func Recover(w *wal, t *Tree)error{
	entries, err := w.Read()
	if err != nil && err !=ErrClosed {
		panic(err)
	}
	if err == nil || err == ErrClosed{
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
// type Options struct{
// 	FileSize int
// 	// Perms represents the datafiles modes and permission bits
// 	DirPerms os.FileMode
// 	FilePerms os.FileMode
// }
// var DefaultOptions = &Options{
// 	FileSize:  20971520,//20 MB 
// 	DirPerms:  0750,// Permissions for the created directories
// 	FilePerms: 0640,// Permissions for the created  files

// }
// type Log struct{
// 	path string 
// 	files []*file 
// 	closed bool
// 	sfile *os.File
// 	corrupt bool
// 	opts Options
// }
// type file struct {
// 	path string
// 	entries []byte

// }

// func Open(path string, opts *Options)(*Log , error){
// 	if opts == nil {
// 		opts = DefaultOptions
// 	}
	

// 	if opts.FileSize <= 0 {
// 		opts.FileSize = DefaultOptions.FileSize
// 	}
// 	if opts.DirPerms == 0 {
// 		opts.DirPerms = DefaultOptions.DirPerms
// 	}
// 	if opts.FilePerms == 0 {
// 		opts.FilePerms = DefaultOptions.FilePerms
// 	}
// 	path, err := filepath.Abs(path)
// 	if err != nil {
// 		return nil, err
// 	}
// 	l := &Log{path: path, opts: *opts}

// 	if err := os.MkdirAll(path, opts.DirPerms); err != nil {
// 		return nil, err
// 	}
// 	return l, nil

// }

