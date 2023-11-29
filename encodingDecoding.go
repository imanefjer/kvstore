package main 

import 	"encoding/binary"

// encodeInt converts an integer value into a 4-byte big-endian encoded byte slice.
func encodeInt(x int)[]byte{
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], uint32(x))
	return encoded[:]
}
// decodeInt converts a 4-byte big-endian encoded byte slice into an integer value.
func decodeInt(encoded []byte) int {
	return int(binary.BigEndian.Uint32(encoded))
}
// encodeNum converts an integer value into a 2-byte big-endian encoded byte slice.
func encodeNum(v int)[]byte {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], uint16(v))
	return encoded[:]
}
// decodeNum converts a 2-byte big-endian encoded byte slice into an integer value.
func decodeNum(encoded []byte) int {
	return int(binary.BigEndian.Uint16(encoded))
}
