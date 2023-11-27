package main

import (
	"bytes"
	// "encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"
)

var (
	//ErrKeynotfound is returned when the lod is corrupt
	ErrKeynotfound = errors.New("key not found")
	//ErrDeleted is returned when the key is deleted
	ErrDeleted = errors.New("key deleted")
	//ErrCorrupt is returned when the Sstable is corrupt
	ErrCorrupt = errors.New("corrupt sstable")
	maxFiles   = 4
)
type SStable struct {
	file        io.ReadWriteSeeker
	magicNumber [4]byte
	smallestKey []byte
	largestKey  []byte
	entryCount  int
	version     int
	checksum    int
	name        string
}
type SStables struct {
	sstables     []*SStable
	path         string //path to the sstable directory
	mu           sync.RWMutex
	numOfSStable int
}

// The NewSST function creates a new SStables object by checking if the directory where we store the sstfiles exists, creating it if
// it doesn't, and then loading any existing sstable files.
func NewSST(path string) (*SStables, error) {
	// Open the directory
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// Directory does not exist, create it
		err := os.Mkdir(path, 0755) 
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	//opening the directory
	dir, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	// Read the directory contents
	files, _ := dir.Readdir(-1)
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
	//closing the directory
	err = dir.Close()
	if err != nil {
		return nil, err
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
// Load all SSTables from a given directory
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
		//we load the sstfiles by creating a new sstable if it has the correct info and in the correct format
		// 
		path1 := fmt.Sprintf(path + "/" + file.Name())
		sstable, err := openSStable(path1)
		if err == ErrCorrupt{
			continue
		}
		if err != nil {
			return nil, err
		}
		sstable.name = path1
		sstables = append(sstables, sstable)
	}
	return sstables, nil
}

func openSStable(path string) (*SStable, error) {
	var content bytes.Buffer
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	// We read the file up to its size minus four, as the checksum within the file
	// was calculated only for the content that precedes its writing.
	f := io.LimitReader(file, fileInfo.Size()-4)
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(&content, f)
	if err != nil {
		return nil, err
	}
	checksum := crc32.ChecksumIEEE(content.Bytes())

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	//read magic number
	var magicNumber [4]byte
	if _, err := file.Read(magicNumber[:]); err != nil {
		return nil, err
	}
	if decodeInt(magicNumber[:]) != 1234 {
		fmt.Println(1)
		return nil, ErrCorrupt
	}
	//read the entry count
	var entryCount [4]byte
	if _, err := file.Read(entryCount[:]); err != nil {
		return nil, err
	}
	//read the smallest key
	var smallestKeyL [4]byte
	if _, err := file.Read(smallestKeyL[:]); err != nil {
		return nil, err
	}
	smallestKeyLen := decodeInt(smallestKeyL[:])
	smallestKey := make([]byte, smallestKeyLen)
	if _, err = file.Read(smallestKey); err != nil {
		return nil, err
	}
	//read the largest key
	var largestKeyL [4]byte
	if _, err := file.Read(largestKeyL[:]); err != nil {
		return nil, err
	}
	largestKeyLen := decodeInt(largestKeyL[:])
	largestKey := make([]byte, largestKeyLen)
	if _, err = file.Read(largestKey); err != nil {
		return nil, err
	}
	//read the version
	var versionEncoded [2]byte
	if _, err := file.Read(versionEncoded[:]); err != nil {
		return nil, err
	}
	version := decodeNum(versionEncoded[:])

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	
	offset := fileInfo.Size() - 4
	_, err = file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	var fileChecksum [4]byte
	if _, err := file.Read(fileChecksum[:]); err != nil {

		return nil, err
	}
	checksumUint32 := decodeInt(fileChecksum[:])

	if checksum != uint32(checksumUint32) {
		fmt.Println(2)
		return nil, errors.New("corrupt sstable")
	}

	sstable := &SStable{
		magicNumber: magicNumber,
		smallestKey: smallestKey,
		largestKey:  largestKey,
		entryCount:  decodeInt(entryCount[:]),
		version:     version,
		checksum:    checksumUint32,
		file:        file,
	}

	return sstable, nil
}

// to search in the sstables from the newest to the oldest in time for a specific key
//the way the entries /nodes ot the tree will be written in the sstfile
func (node *Node) format() []byte {
	var marker []byte
	if node.marker {
		marker = encodeNum(1)
	} else {
		marker = encodeNum(0)
	}
	len1 := len(node.Key)
	len2 := len(node.Value)
	keyLen := encodeInt(len1)
	valueLen := encodeInt(len2)
	key := node.Key
	value := node.Value
	res := make([]byte, len1+len2+10)
	copy(res[0:2], marker)
	copy(res[2:6], keyLen)
	copy(res[6:len1+6], key)
	copy(res[len1+6:len1+10], valueLen)
	copy(res[len1+10:len1+len2+10], value)
	return res
}

// when we flush to disk we create a new sstable that contains the content of the tree
func (s *SStables) Flush(tree *Tree) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	//create a new sstable
	path := fmt.Sprintf(s.path + "/" + s.Name())
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return err
	}
	//write the magic number
	magicNumber := encodeInt(1234)
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
	_, err = file.WriteString(string(tree.Min()))
	if err != nil {
		return err
	}

	//write the largest key
	maxKey := encodeInt(len((tree.Max())))
	if _, err := file.Write(maxKey); err != nil {
		return err
	}
	_, err = file.WriteString(string(tree.Max()))
	if err != nil {
		return err
	}

	//write the version
	version := encodeNum(1)
	if _, err := file.Write(version); err != nil {
		return err
	}
	// We iterate through the tree in ascending order, writing each node into the file
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
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	var content bytes.Buffer

	_, err = io.Copy(&content, file)

	if err != nil {
		return err
	}
	checksum := crc32.ChecksumIEEE(content.Bytes())
	checksumBytes := encodeInt(int(checksum))
	if _, err := file.Write(checksumBytes); err != nil {
		return err
	}
	//close the file
	if err := file.Close(); err != nil {
		return err
	}
	s.numOfSStable++
	//add the new sstable to the sstables
	var magicNumber1 [4]byte
	copy(magicNumber[:], encodeInt(1234))

	s.sstables = append(s.sstables, &SStable{
		magicNumber: magicNumber1,
		smallestKey: tree.Min(),
		largestKey:  tree.Max(),
		entryCount:  tree.Len(),
		version:     1,
		checksum:    int(checksum),
		file:        file,
		name:        path,
	})
	// If the count of sstfiles reaches the maximum allowable number of files (maxFiles)
	// we initiate the  compaction process
	if (s.numOfSStable == maxFiles){
		fmt.Println("hana")
		time.Sleep(1 * time.Second)

		err = s.Compact()
		if err != nil {
			return err
		}
	}
	return nil
}

//To name of the files
func (s *SStables) Name() string{
	if s == nil {
		return "Invalid SStables (nil)"
	}
	currentTime := time.Now()        
	path :=  fmt.Sprintf("file%s.sst", currentTime.Format("2006_01_02_15_04_05"))
	return path
}

func (s *SStables) Search(key []byte) ([]byte, error) {
	// When searching for a key in the SSTables, we begin with the newest file and so on
	for i := len(s.sstables) - 1; i >= 0; i-- {
		if bytes.Compare(key, s.sstables[i].smallestKey[:]) >= 0 && bytes.Compare(key, s.sstables[i].largestKey[:]) <= 0 {
			value, err := s.sstables[i].search(key)
			// If the key is marked as deleted, an error is returned.
			if err == ErrDeleted {
				return nil, err
			}
			// search in the next SSTable
			if err == ErrKeynotfound {
				continue
			}
			if err != nil {
				return nil, err
			}
			return value, nil
		}
	}

	return nil, ErrKeynotfound
}

func (s *SStable) search(key []byte) ([]byte, error) {
	f, err := os.OpenFile(s.name, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var content bytes.Buffer
	fileInfo, err := f.Stat()
	// We read the file up to its size minus four, as the checksum within the file
	// was calculated only for the content that precedes its writing.
	file := io.LimitReader(f, fileInfo.Size()-4)
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(&content, file)
	if err != nil {
		return nil, err
	}
	checksum := crc32.ChecksumIEEE(content.Bytes())
	if (checksum != uint32(s.checksum)){
		return nil, ErrCorrupt
	}
	// go to the block where the keys and values are stored
	offset := 4 + 4 + 4 + 4 + 2 + len(s.largestKey) + len(s.smallestKey)
	_, err = f.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return nil, err
	}
	//check if the key is in the file
	entryCount := s.entryCount
	for i := 0; i < entryCount; i++ {
		var marker [2]byte
		_, err := f.Read(marker[:])
		if err != nil {
			return nil, err
		}
		var keyLen [4]byte
		if _, err := f.Read(keyLen[:]); err != nil {
			return nil, err
		}
		keyLenInt := decodeInt(keyLen[:])
		key1 := make([]byte, keyLenInt)
		if _, err := f.Read(key1); err != nil {
			return nil, err
		}
		//if the current key is bigger  than the key we are looking for then the key is not in this file
		//as they are written in an ascending way
		if bytes.Compare(key, key1) < 0 {
			return nil, ErrKeynotfound
		}
		if bytes.Equal(key1, key) {
			if decodeNum(marker[:]) == 1 {
				var valueLen [4]byte
				if _, err := f.Read(valueLen[:]); err != nil {
					return nil, err
				}
				valueLenInt := decodeInt(valueLen[:])
				value1 := make([]byte, valueLenInt)

				if _, err := f.Read(value1); err != nil {
					return nil, err
				}
				return value1, nil
			} else {
				return nil, ErrDeleted
			}
		}
		var valueLen [4]byte
		if _, err := f.Read(valueLen[:]); err != nil {
			return nil, err
		}
		valueLenInt := decodeInt(valueLen[:])

		value := make([]byte, valueLenInt)
		if _, err := f.Read(value); err != nil {
			return nil, err
		}
	}
	return nil, ErrKeynotfound
}

func (s *SStables) Compact() error {

	var newSSts []*SStable
	fmt.Println("9rbt")
	fmt.Println(len(s.sstables))
	for i := 0; i <= s.numOfSStable-2; i += 2 {
		fmt.Println("9rbt")
		fmt.Println(i)
		NewSst, err := s.merge(s.sstables[i], s.sstables[i+1])
		if err != nil {
			return err
		}
		time.Sleep(1 * time.Second)
		fmt.Println("daz")
		newSSts = append(newSSts, NewSst)
	}
	s.numOfSStable = s.numOfSStable / 2
	s.sstables = newSSts

	return nil
}

func (s *SStables) merge(s1 *SStable, s2 *SStable) (*SStable, error) {
	//the new sstable will have the same magicnumber and version
	//the smallest key will be the smallest key of the two sstables
	//the largest key will be the largest key of the two sstables
	//the entry count will be the sum of the entry count of the two sstables
	path := fmt.Sprintf(s.path + "/" + s.Name())
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return nil, err
	}

	f1, err := os.OpenFile(s1.name, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer f1.Close()
	f2, err := os.OpenFile(s2.name, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer f2.Close()
	//check if the file was not corrupted

	var content1 bytes.Buffer
	fileInfo1, err := f1.Stat()
	//check if the files were not corrupted
	file1 := io.LimitReader(f1, fileInfo1.Size()-4)
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(&content1, file1)
	if err != nil {
		return nil, err
	}
	checksum1 := crc32.ChecksumIEEE(content1.Bytes())
	if (checksum1 != uint32(s1.checksum)){
		return nil, ErrCorrupt
	}
	_, err = f1.Seek(0,io.SeekStart)
	if err != nil {
		return nil, err
	}
	//checksum 
	var content2 bytes.Buffer
	fileInfo2, err := f2.Stat()
	file2 := io.LimitReader(f2, fileInfo2.Size()-4)
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(&content2, file2)
	if err != nil {
		return nil, err
	}
	checksum2 := crc32.ChecksumIEEE(content2.Bytes())
	if (checksum2 != uint32(s2.checksum)){
		return nil, ErrCorrupt
	}
	_, err = f2.Seek(0,io.SeekStart)
	if err != nil {
		return nil, err
	}
	// read the magicnumber
	var magicNumber [4]byte
	if _, err := f1.Read(magicNumber[:]); err != nil {
		return nil, err
	}
	//write it in the new file
	if _, err := file.Write(magicNumber[:]); err != nil {
		return nil, err
	}
	var magicNumber2 [4]byte

	if _, err := f2.Read(magicNumber2[:]); err != nil {
		return nil, err
	}
	//read the entry count
	var entryCount1 [4]byte
	if _, err := f1.Read(entryCount1[:]); err != nil {
		return nil, err
	}
	//decode
	entryCountInt1 := decodeInt(entryCount1[:])
	var entryCount2 [4]byte
	if _, err := f2.Read(entryCount2[:]); err != nil {
		return nil, err
	}
	//decode
	entryCountInt2 := decodeInt(entryCount2[:])

	entryCount := 0
	//the smallest key
	var smallestKey1 [4]byte
	if _, err := f1.Read(smallestKey1[:]); err != nil {
		return nil, err
	}
	smallestKeyLen1 := decodeInt(smallestKey1[:])
	smallestKey1Bytes := make([]byte, smallestKeyLen1)
	if _, err := f1.Read(smallestKey1Bytes); err != nil {
		return nil, err
	}
	var smallestKey2 [4]byte
	if _, err := f2.Read(smallestKey2[:]); err != nil {
		return nil, err
	}
	smallestKeyLen2 := decodeInt(smallestKey2[:])
	smallestKey2Bytes := make([]byte, smallestKeyLen2)
	if _, err := f2.Read(smallestKey2Bytes); err != nil {
		return nil, err
	}
	var smallestKey []byte
	if bytes.Compare(smallestKey1Bytes, smallestKey2Bytes) < 0 {
		smallestKey = smallestKey1Bytes
	} else {
		smallestKey = smallestKey2Bytes
	}
	
	//the largest key
	var largestKey1 [4]byte
	if _, err := f1.Read(largestKey1[:]); err != nil {
		return nil, err
	}
	largestKeyLen1 := decodeInt(largestKey1[:])
	largestKey1Bytes := make([]byte, largestKeyLen1)
	if _, err := f1.Read(largestKey1Bytes); err != nil {
		return nil, err
	}
	var largestKey2 [4]byte
	if _, err := f2.Read(largestKey2[:]); err != nil {
		return nil, err
	}
	largestKeyLen2 := decodeInt(largestKey2[:])
	largestKey2Bytes := make([]byte, largestKeyLen2)
	if _, err := f2.Read(largestKey2Bytes); err != nil {
		return nil, err
	}
	var largestKey []byte
	if bytes.Compare(largestKey1Bytes, largestKey2Bytes) > 0 {
		largestKey = largestKey1Bytes
	} else {
		largestKey = largestKey2Bytes
	}
	
	//the version
	var version [2]byte
	if _, err := f1.Read(version[:]); err != nil {
		return nil, err
	}
	
	_, err = f2.Seek(2, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	tree := Tree{}
	//write the keyvalues in an ordred way as it already is in f1 and f2
	for i := 0; i < entryCountInt1; i++ {
		var marker [2]byte
		if _, err := f1.Read(marker[:]); err != nil {
			return nil, err
		}
		//decode marker
		markerInt := decodeNum(marker[:])
		//read the key length
		var keyLen [4]byte
		if _, err := f1.Read(keyLen[:]); err != nil {
			return nil, err
		}
		//decode key length
		keyLenInt := decodeInt(keyLen[:])
		//read the key
		key := make([]byte, keyLenInt)
		if _, err := f1.Read(key); err != nil {
			return nil, err
		}
		//read the value length
		var valueLen [4]byte
		if _, err := f1.Read(valueLen[:]); err != nil {
			return nil, err
		}
		//decode value length
		valueLenInt := decodeInt(valueLen[:])
		//read the value
		value := make([]byte, valueLenInt)
		if _, err := f1.Read(value); err != nil {
			return nil, err
		}

		if markerInt == 1 {
			tree.Set(key, value)

		}
	}

	for i := 0; i < entryCountInt2; i++ {
		var marker [2]byte
		if _, err := f2.Read(marker[:]); err != nil {
			return nil, err
		}
		//decode marker
		markerInt := decodeNum(marker[:])
		//read the key length
		var keyLen [4]byte
		if _, err := f2.Read(keyLen[:]); err != nil {
			return nil, err
		}
		//decode key length
		keyLenInt := decodeInt(keyLen[:])
		//read the key
		key := make([]byte, keyLenInt)
		if _, err := f2.Read(key); err != nil {
			fmt.Println("test")

			return nil, err
		}

		//read the value length
		var valueLen [4]byte
		if _, err := f2.Read(valueLen[:]); err != nil {
			return nil, err
		}
		//decode value length
		valueLenInt := decodeInt(valueLen[:])
		//read the value
		value := make([]byte, valueLenInt)
		if _, err := f2.Read(value); err != nil {
			return nil, err
		}

		if markerInt == 1 {
			tree.Set(key, value)
		}
	}
	entryCount = tree.Len()
	//write the entry count
	if _, err := file.Write(encodeInt(entryCount)); err != nil {
		return nil, err
	}
	//smallest key
	smallestKeyLen := encodeInt(len(smallestKey))
	if _, err := file.Write(smallestKeyLen); err != nil {
		return nil, err
	}
	if _, err := file.Write(smallestKey); err != nil {
		return nil, err
	}
	//largest key
	largestKeyLen := encodeInt(len(largestKey))
	if _, err := file.Write(largestKeyLen); err != nil {
		return nil, err
	}
	if _, err := file.Write(largestKey); err != nil {
		return nil, err
	}
	if _, err := file.Write(version[:]); err != nil {
		return nil, err
	}
	//write the keyvalues in an ordred way as it already is in f1 and f2
	for it := tree.Iterator(); it.HasNext(); {
		currNode, err := it.Next()
		if err != nil {
			return nil, err
		}
		nodeFormat := currNode.format()
		if _, err := file.Write(nodeFormat); err != nil {
			return nil, fmt.Errorf("failed to write to disk table %d: %w", s.numOfSStable, err)
		}
	}
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	var content bytes.Buffer

	_, err = io.Copy(&content, file)

	if err != nil {
		return nil, err
	}
	checksum := crc32.ChecksumIEEE(content.Bytes())
	checksumBytes := encodeInt(int(checksum))
	if _, err := file.Write(checksumBytes); err != nil {
		return nil, err
	}
	//close the file
	if err := file.Close(); err != nil {
		return nil, err
	}
	newSSt := SStable{
		magicNumber: magicNumber,
		smallestKey: smallestKey,
		largestKey:  largestKey,
		entryCount:  entryCountInt1 + entryCountInt2,
		version:     s1.version,
		checksum:    int(checksum),
		file:        file,
		name:        path,
	}
	//close f1 and f2
	err = f1.Close()
	if err != nil {
		return nil, err
	}
	err = f2.Close()
	if err != nil {
		return nil, err
	}
	err = os.Remove(s1.name)
	if err != nil {
		return nil, err
	}
	err = os.Remove(s2.name)
	if err != nil {
		return nil, err
	}
	return &newSSt, nil
}
