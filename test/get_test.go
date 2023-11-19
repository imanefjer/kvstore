package main

import (
	"testing"
	// "bytes"
)


func TestGet(t *testing.T) {
	// f := fileDB{
	// 	file:  bytes.NewBufferString(""),
	// 	Term:  byte('#'),
	// 	Bsize: 10,
	// }
	// f[0] = []byte("foo")
	// f[1] = []byte("bar")
	tests := []struct {
		Name     string
		key       []byte
		ExError  bool
		ExOutput []byte
	}{
		{
			Name:     "Test normal case",
			key:      []byte("foo"),
			ExError:  false,
			// ExOutput: f.Get([]byte("foo")),
		},
		{
			Name:     "Test key doesn't exist",
			key:      []byte("key"),			
			ExError:  true,
			ExOutput: []byte{},
		},
		{
			Name:     "value diff than expexcted",
			key:      []byte("foo"),
			ExError:  true,
			ExOutput: []byte("foo"),
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			// _, err := testDb.Get([]byte("key"))
			// if err == nil {
			// 	t.Fatalf("Found key in empty DB")
			// }

			// // res, err := f.Set(test.key, test.ExOutput)

			// if !test.ExError && err != nil {
			// 	t.Fatal("error")
			// }
			// if test.ExError && err == nil {
			// 	t.Fatal("error")
			// }

			// if !test.ExError && res != test.ExOutput {
			// 	t.Fatalf("Expected value %d, is different than actual value %d", test.ExOutput, res)
			// }
		})
	}
}