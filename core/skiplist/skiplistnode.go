package skiplist

import (
	"nakevaleng/core/record"
)

type SkiplistNode struct {
	Value record.Record
	Next  []*SkiplistNode
}

func NewNode(key, value []byte, level int) SkiplistNode {
	newNode := SkiplistNode{}
	newNode.Value = record.New(key, value)
	newNode.Next = make([]*SkiplistNode, level)

	return newNode
}

func NewNodeFromRecord(rec record.Record, level int) SkiplistNode {
	return SkiplistNode{
		Value: rec,
		Next:  make([]*SkiplistNode, level),
	}
}
