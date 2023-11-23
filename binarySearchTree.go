package main

import (
	"bytes"
	"errors"
	"fmt"
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
			n.Left = &Node{Key: key, Value: value, marker: true, Parent: n}
			return nil
		}
		return n.Left.Set(key, value)
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

// The `Get` function is used to search for a specific value in a binary search tree. It takes a value
// as input and returns the corresponding data associated with that value, along with a boolean
// indicating whether the value was found or not.
func (n *Node) Get(key []byte) ([]byte, bool) {
	if n == nil {
		return nil, false
	}
	switch {
	case bytes.Equal(key, n.Key):
		if n.marker{
			return n.Value, true
		}
		return nil, false
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

// to delete a node we just look for it and make the marker false 
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
		if !n.marker {
			return errors.New("the key is already deleted")
		}
		n.marker = false
		return nil
		// // for a leaf
		// if n.Left == nil && n.Right == nil {
		// 	n.replaceNode(parent, nil)
		// 	return nil
		// }
		// // for a node with one child
		// if n.Left == nil {
		// 	n.replaceNode(parent, n.Right)
		// 	return nil
		// }
		// if n.Right == nil {
		// 	n.replaceNode(parent, n.Left)
		// 	return nil
		// }
		// replNode, replParent := n.Left.findMax(n)
		// n.Key = replNode.Key
		// n.Value = replNode.Value
		// return replNode.Del(replNode.Key, replParent)
	}
}

func (t *Tree) Set(value, data []byte) error {
	if t.Root == nil {
		t.Root = &Node{Key: value, Value: data, marker: true}
		return nil
	}
	return t.Root.Set(value, data)
}

// To get a key in tree we search if it the root otherwise we call the node.get implemented earlier
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

func Print(tree *Tree){
	if (tree.Root == nil){
		fmt.Println("empty tree")
	}
	printTree(tree.Root)
}
func printTree (node *Node){
	if node == nil{
		return
	}
	printTree(node.Left)
	mar:= "true"
	if !node.marker{
		mar ="false"
	}
	fmt.Println("key: ",string(node.Key)," value: ",string(node.Value), "marker",mar )
	printTree(node.Right)
}