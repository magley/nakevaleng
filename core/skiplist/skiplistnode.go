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

// newNode() creates a SkiplistNode from the given record structure with a given number of levels.
//	rec  	Record object stored in the node. Record.Key is used for searching in the skiplist
//	level	Number of levels, valid values are non-zero positive integers
func newNode(rec record.Record, level int) SkiplistNode {
	return SkiplistNode{
		Data: rec,
		Next: make([]*SkiplistNode, level),
	}
}

// newNodeEmpty() creates a SkiplistNode whose record stores no data.
// Use this function to create header nodes for the skiplist.
// 	level	Number of levels, valid values are non-zero positive integers
func newNodeEmpty(level int) SkiplistNode {
	return SkiplistNode{
		Data: record.New([]byte(""), []byte("")),
		Next: make([]*SkiplistNode, level),
	}
}
