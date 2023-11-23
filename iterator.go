package main

import "errors"


type Iterator struct {
	next *Node
}

// Iterator returns a stateful iterator that traverses the tree
// in ascending Key order.
func (t *Tree) Iterator() *Iterator {
	next := t.Root
	if next != nil {
		for next.Left != nil {
			next = next.Left
		}
	}

	return &Iterator{next}
}

// HasNext returns true if there is a next element.
func (it *Iterator) HasNext() bool {
	return it.next != nil
}

// Next function is used to retrieve the next key-value pair in the iterator and advance the
// iterator to the next position.
func (it *Iterator) Next() (*Node, error) {
	if !it.HasNext() {
		return nil, errors.New("cannot call next on a nil iterator")
	}

	current := it.next
	if it.next.Right != nil {
		it.next = it.next.Right
		for it.next.Left != nil {
			it.next = it.next.Left
		}

		return current, nil
	}

	for {
		if it.next.Parent == nil {
			it.next = nil

			return current, nil
		}
		if it.next.Parent.Left == it.next {
			it.next = it.next.Parent

			return current, nil
		}
		it.next = it.next.Parent
	}
}