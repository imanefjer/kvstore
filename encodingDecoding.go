package main 

import 	"encoding/binary"

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
func encodeNum(v int)[]byte {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], uint16(v))
	return encoded[:]
}
func decodeNum(encoded []byte) int {
	return int(binary.BigEndian.Uint16(encoded))
}
