package main

import (
	"bytes"
	"errors"
	// "fmt"
)

type Node struct {
	marker bool //0 to represent del and 1 to represent set
	Key   []byte
	Value []byte
	Left  *Node
	Right *Node
	Parent *Node
}
type Tree struct {
	Root *Node
}
//set a value in the BST by looking for the right place to put the key if id not already in the
//tree 
func (n *Node) Set(key, value []byte) error {
	if n == nil {
		return errors.New("cannot insert a value into a nil tree")
	}
	switch {
	//if the keys are equal we update the value
	case bytes.Equal(key, n.Key): 
		n.Value = value
		return nil
	//if the key is less than the current node key we search in the left subtree
	case bytes.Compare(key, n.Key) == -1:
		if n.Left == nil {
			n.Left = &Node{Key: key, Value: value, marker: true, Parent: n}
			return nil
		}
		return n.Left.Set(key, value)
	//if the key is greater than the current node key we search in the right subtree
	case bytes.Compare(key, n.Key) == 1:
		if n.Right == nil {
			n.Right = &Node{Key: key, Value: value, marker: true, Parent: n}
			return nil
		}
		return n.Right.Set(key, value)
	default:
		return nil
	}
}
//we get the value of a given a key by looking and comparing the key with the current node key 
//and checking if the maker is 1 (the value is not deleted)
func (n *Node) Get(key []byte) ([]byte, error) {
	if n == nil {
		return nil, ErrKeynotfound
	}
	switch {
	case bytes.Equal(key, n.Key):
		if n.marker{
			return n.Value, nil
		}
		return nil, ErrDeleted
	case bytes.Compare(key, n.Key) == -1:
		return n.Left.Get(key)
	default:
		return n.Right.Get(key)
	}
}

//the max key in the tree
func (t Tree) Max() []byte {
	if t.Root == nil {
		return nil
	}
	return t.Root.max()
}
func (n *Node) max() []byte {
	if n.Right == nil {
		return n.Key
	}
	return n.Right.max()
}
//the min key in the tree
func (t *Tree) Min() []byte {
	if t.Root == nil {
		return nil
	}
	return t.Root.min()
}
func (n *Node) min() []byte {
	if n.Left == nil {
		return n.Key
	}
	return n.Left.min()
}

//Len of the tree
func (t Tree)Len() int{
	if t.Root==nil{
		return 0
	}
	return t.Root.len()
}
func (n *Node)len()int{
	if n==nil{
		return 0
	}
	return 1+n.Left.len()+n.Right.len()
}
//to delete a key if a tree we check if it exists and if it does and the maker is 1 
// we change the marker to 0
func (n *Node) Del(key []byte, parent *Node) error {
	if n == nil {
		return ErrKeynotfound
	}
	switch {
	case bytes.Compare(key, n.Key) == -1:
		return n.Left.Del(key, n)
	case bytes.Compare(key, n.Key) == 1:
		return n.Right.Del(key, n)
	default:
		if !n.marker {
			return ErrDeleted
		}
		n.marker = false
		return nil
	}
}
 

func (t *Tree) Set(value, data []byte) error {
	if t.Root == nil {
		t.Root = &Node{Key: value, Value: data, marker: true}
		return nil
	}
	return t.Root.Set(value, data)
}

func (t *Tree) Get(key []byte) ([]byte, error) {
	if t.Root == nil {
		return nil, ErrKeynotfound
	}
	return t.Root.Get(key)
}
func (t *Tree) Del(key []byte) error {
	//deleting an empty tree
	if t.Root == nil {
		return ErrKeynotfound
	}
	// we add parent to deal with the case of deleting the root also
	parent := &Node{Right: t.Root}
	err := t.Root.Del(key, parent)
	if err != nil {
		return err
	}
	//In this case, we are dealing with a situation where the tree contains
	// only the root, and we deleted so the tree will be empty.
	if parent.Right == nil {
		t.Root = nil
	}
	return nil
}

// The Ascend function is used to traverse the binary search tree in ascending order. 
func (t *Tree) Ascend(visit func(key []byte, value []byte) bool) {
	ascendInOrder(t.Root, visit)
}

func ascendInOrder(node *Node, visit func(key []byte, value []byte) bool) bool {
	if node == nil {
		return true
	}

	if !ascendInOrder(node.Left, visit) {
		return false
	}

	if !visit(node.Key, node.Value) {
		return false
	}

	if !ascendInOrder(node.Right, visit) {
		return false
	}

	return true
}
// The Reinitialize function is used to reset the binary search tree to an empty state.
func (tree *Tree)Reinitialize() error{
	if tree.Root == nil{
		return errors.New("the tree is already empty")
	}
	tree.Root = nil
	return nil
}
// The SetDeletedKey function is used to insert a new node into a binary search tree with a marker
// set to false.  
func (tree *Tree) SetDeletedKey(key , value[]byte) error{
	if tree.Root == nil{
		tree.Root = &Node{Key: key, Value: value, marker: false}
		return nil
	}
	return tree.Root.SetDeletedKey(key, value)
}
func (n *Node) SetDeletedKey(key , value[]byte) error{
	if n == nil{
		return errors.New("cannot insert a value into a nil tree")
	}
	switch {
	case bytes.Equal(key, n.Key):
		n.Value = value
		n.marker = false
		return nil
	case bytes.Compare(key, n.Key) == -1:
		if n.Left == nil {
			n.Left = &Node{Key: key, Value: value, marker: false, Parent: n}
			return nil
		}
		return n.Left.SetDeletedKey(key, value)
	case bytes.Compare(key, n.Key) == 1:
		if n.Right == nil {
			n.Right = &Node{Key: key, Value: value, marker: false, Parent: n}
			return nil
		}
		return n.Right.SetDeletedKey(key, value)
	default:
		return nil
	}
}