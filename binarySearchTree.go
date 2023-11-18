package main

import (
	"bytes"
	"errors"
)

type Node struct {
	Key []byte
	Value  []byte
	Left  *Node
	Right *Node
}
type Tree struct {
	Root *Node
}

// This function is used to insert a new node into a binary search tree by respecting the rules in the binary search tree

func (n *Node) Set(key, value []byte) error {
	if n == nil {
		return errors.New("cannot insert a value into a nil tree")
	}
	switch {
	case bytes.Equal(key, n.Key):
		n.Value = value
		return nil
	case bytes.Compare(key, n.Key) == -1:
		if n.Left == nil {
			n.Left = &Node{Key: key, Value: value}
			return nil
		}
		return n.Left.Set(key, value)
	case bytes.Compare(key, n.Key) == 1:
		if n.Right == nil {
			n.Right = &Node{Key: key, Value: value}
			return nil
		}
		return n.Right.Set(key, value)
	default:
		return nil
	}
}

// The `Get` function is used to search for a specific value in a binary search tree. It takes a value
// as input and returns the corresponding data associated with that value, along with a boolean
// indicating whether the value was found or not.
func (n *Node) Get(key []byte) ([]byte, bool) {
	if n == nil {
		return nil, false
	}
	switch {
	case bytes.Equal(key, n.Key):
		return n.Value, true
	case bytes.Compare(key, n.Key) == -1:
		return n.Left.Get(key)
	default:
		return n.Right.Get(key)
	}
}

//  deleting in a binary search tree is quiet complicated in the case when we want to delete an inner node:
// if its a right child of  its parent then we search for the largest value in its left subtreee  we replace the node's value
// with this value (largest value== Lvalue) if it's  (largest value) then we call delete on this node Lvalue. If it is the left
// child of it's parent node then we search for the smallest value in the node's right subtree

func (n *Node) findMax(parent *Node) (*Node, *Node) {
	if n == nil {
		return nil, parent
	}
	if n.Right == nil {
		return n, parent
	}
	return n.Right.findMax(n)

}

func (n *Node) replaceNode(parent, newValue *Node) error {
	if n == nil {
		return errors.New("we can not replace on a nil node")
	}
	if n == parent.Left {
		parent.Left = newValue
		return nil
	}
	parent.Right = newValue
	return nil
}
func (n *Node) Del(key []byte, parent *Node) error {
	if n == nil {
		return errors.New("we can not delete a nil node")
	}
	switch {
	case bytes.Compare(key, n.Key) == -1:
		return n.Left.Del(key, n)
	case bytes.Compare(key, n.Key) == 1:
		return n.Right.Del(key, n)
	default:
		// for a leaf
		if n.Left == nil && n.Right == nil {
			n.replaceNode(parent, nil)
			return nil
		}
		// for a node with one child
		if n.Left == nil {
			n.replaceNode(parent, n.Right)
			return nil
		}
		if n.Right == nil {
			n.replaceNode(parent, n.Left)
			return nil
		}
		replNode, replParent := n.Left.findMax(n)
		n.Key = replNode.Key
		n.Value = replNode.Value
		return replNode.Del(replNode.Key, replParent)
	}
}

func (t *Tree) Set(value, data []byte) error {
	if t.Root == nil {
		t.Root = &Node{Key: value, Value: data}
		return nil
	}
	return t.Root.Set(value, data)
}
//To get a key in tree we search if it the root otherwise we call the node.get implemented earlier  
func (t *Tree) Get(key []byte) ([]byte, bool) {
	if t.Root == nil {
		return nil, false
	}
	return t.Root.Get(key)
}
func (t *Tree) Del(key []byte) error {
	//deleting an empty tree
	if t.Root == nil {
		return errors.New("the tree is already empty")
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
