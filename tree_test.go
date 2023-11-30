package main

import (
	"bytes"
	"testing"
)

func TestGetTree(t *testing.T) {
	tests := []struct {
		Name     string
		Key      []byte
		Value    []byte
		ExError  bool
		ExOutput []byte
	}{
		{
			Name:     "Test normal case",
			Key:      []byte("1"),
			Value:    []byte("value1"),
			ExError:  false,
			ExOutput: []byte("value1"),
		},
		{
			Name:     "Test key doesn't exist",
			Key:      []byte("8"),
			ExError:  true,
			ExOutput: []byte{},
		},
		{
			Name:     "Value different than expected",
			Key:      []byte("1"),
			Value:    []byte("value2"),
			ExError:  true,
			ExOutput: []byte("0"),
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			tree := Tree{}

			// Test Get on an empty tree
			_, err := tree.Get(test.Key)
			if err == nil {
				t.Fatal("Found key in empty DB")
			}

			// Test Set
			tree.Set([]byte("1"), test.Value)

			// Test Get
			res, err := tree.Get(test.Key)
			// Check errors
			if !test.ExError && err != nil {
				t.Fatal("Unexpected error:", err)
			}
			err1 := bytes.Equal(res, test.ExOutput)
			if test.ExError && err == nil && err1{
				t.Fatal("Expected an error but got none")

			}
			if !test.ExError && !bytes.Equal(res, test.ExOutput){
				t.Fatalf("Expected value %d, is different than actual value %d", test.ExOutput, res)
			}
			
		})
	}
}

func TestTreeSet(t *testing.T) {
	tests := []struct {
		Name     string
		Key      []byte
		Value    []byte
		ExError  bool
		ExOutput []byte
	}{
		{
			Name:     "Test normal case",
			Key:      []byte("1"),
			Value:    []byte("value1"),
			ExError:  false,
			ExOutput: []byte("value1"),
		},
		{
			Name:     "Test key already exists",
			Key:      []byte("1"),
			Value:    []byte("value2"),
			ExError:  false,
			ExOutput: []byte("value2"),
		},
		{
			Name:     "Test empty value",
			Key:      []byte("2"),
			Value:    []byte{},
			ExError:  false,
			ExOutput: []byte{},
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			tree := Tree{}
			err := tree.Set(test.Key, test.Value)

			// Check errors
			if !test.ExError && err != nil {
				t.Fatal("Unexpected error:", err)
			}
			if test.ExError && err == nil {
				t.Fatal("Expected an error but got none")
			}

			// Test Get after Set
			res, _ := tree.Get(test.Key)
			// Check output values
			if !test.ExError && !bytes.Equal(res, test.ExOutput) {
				t.Fatalf("Expected value %s, but got %s", test.ExOutput, res)
			}
		})
	}
}

func TestTreeDel(t *testing.T) {
	tests := []struct {
		Name     string
		Key      []byte
		Value    []byte
		DelKey   []byte
		ExError  bool
		ExOutput []byte
	}{
		{
			Name:     "Test delete normal case",
			Key:      []byte("1"),
			Value:    []byte("value1"),
			ExError:  false,
			ExOutput: []byte{},
		},
		{
			Name:     "Test delete non-existent key",
			Key:      []byte("2"),
			Value:    []byte("value2"),
			ExError:  true,
			ExOutput: []byte("value2"),
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			tree := Tree{}
			// Set initial value
			err := tree.Set(test.Key, test.Value)

			// Check errors in Set
			if err != nil {
				t.Fatal("Unexpected error in Set:", err)
			}
			// Test Del
			err = tree.Del(test.Key)
			// Check errors in Del
			if !test.ExError && err != nil {
				t.Fatal("Unexpected error in Del:", err)
			}
			// Test Get after Del
			_, err = tree.Get(test.Key)
			if (err == nil){
				t.Fatal("Expected an error but got none")
			}
		
		})
	}
}
