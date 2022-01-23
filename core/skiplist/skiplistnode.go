package skiplist

import (
	"nakevaleng/core/record"
)

// SkiplistNode represents a single node in a Skiplist, specialized for nakevaleng. It stores data
// in the form of a Record object and pointers to the next node relative to this one for each level
// in the skiplist.
type SkiplistNode struct {
	Data record.Record   // The data stored in the node, as a Record object.
	Next []*SkiplistNode // Next[i] is the pointer to the next node (for this node) on level i.
}

// NewNode() creates a SkiplistNode from the given record structure with a given number of levels.
//	rec  	Record object stored in the node. Record.Key is used for searching in the skiplist
//	level	Number of levels, valid values are non-zero positive integers
func NewNode(rec record.Record, level int) SkiplistNode {
	return SkiplistNode{
		Data: rec,
		Next: make([]*SkiplistNode, level),
	}
}

// NewNodeFromKeyVal() creates a SkiplistNode from the given key and val bytes with a given number
// of levels. This function is NOT reccommended for use because the Record object it creates won't
// be identical to the one used in the rest of the engine (for fields timestamp, status etc.). This
// function is only for testing purposes.
//	 `Please use`	NewNode()	`instead of`	NewNodeFromKeyVal()  `except for header nodes`
//
//	key		Key of the node and internal record structure, as byte slice
//	val		Value of the node and internal record structure, as byte slice
//	level	Number of levels, valid values are non-zero positive integers
func NewNodeFromKeyVal(key, value []byte, level int) SkiplistNode {
	newNode := SkiplistNode{}
	newNode.Data = record.New(key, value)
	newNode.Next = make([]*SkiplistNode, level)

	return newNode
}
