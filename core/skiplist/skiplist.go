package skiplist

import (
	"bytes"
	"fmt"
	"math/rand"
	"nakevaleng/core/record"
)

type Skiplist struct {
	Level    int
	LevelMax int
	Header   *SkiplistNode
	Count    int // Number of elements, incuding "removed" ones
}

// New creates an empty Skiplist with height 'level'.
// 	level	Number of available levels in range [0, level).
// Throws an error if specified height is greater than the maximium allowed height or less than 1.
func New(level int, levelmax int) *Skiplist {
	if level > levelmax {
		errMsg := fmt.Sprint("Maximum skiplist height is", levelmax, ", but", level, "was given.")
		panic(errMsg)
	}

	if level <= 0 {
		errMsg := fmt.Sprint("Minimum skiplist height is 1, but", level, "was given.")
		panic(errMsg)
	}

	header := newNodeEmpty(levelmax)

	return &Skiplist{
		Level:    level,
		LevelMax: levelmax,
		Header:   &header,
		Count:    0,
	}
}

// Clear removes all nodes from the Skiplist and resets the number of levels to 1.
func (skiplist *Skiplist) Clear() {
	skiplist.Level = 1
	emptyHeader := newNodeEmpty(skiplist.LevelMax)
	skiplist.Header = &emptyHeader
	skiplist.Count = 0
}

// Write writes the given record into the skiplist. If an element with the same key already exists,
// it gets updated and false is returned. Otherwise, the element is inserted and true is returned.
func (skiplist *Skiplist) Write(rec record.Record) bool {
	node := skiplist.Header
	update := make([]*SkiplistNode, skiplist.LevelMax) // A ptr to a level-i node that'll get relinked.

	// Go from top to bottom level, and find the node with the greatest key less than 'key'.

	for lvl := skiplist.Level - 1; lvl >= 0; lvl-- {
		for node.Next[lvl] != nil && bytes.Compare(rec.Key, node.Next[lvl].Data.Key) == 1 {
			node = node.Next[lvl]
		}
		update[lvl] = node
	}

	node = node.Next[0]

	// Node with given key already exists.

	if !(node == nil || bytes.Compare(rec.Key, node.Data.Key) != 0) {
		node.Data = rec
		return false
	}

	// Determine how many levels the new node will have.

	newNodeLvl := 1

	rnd := rand.Intn(2)
	for rnd != 0 && newNodeLvl < skiplist.LevelMax {
		newNodeLvl += 1
		rnd = rand.Intn(2)
	}

	if newNodeLvl > skiplist.LevelMax {
		newNodeLvl = skiplist.LevelMax
	}

	// If the list doesn't use newNodeLvl-many levels, we'll make it so that it does.
	// "Bonus" nodes live in the header, because 'update' stores nodes that go before our new node.

	if newNodeLvl > skiplist.Level {
		for lvl := skiplist.Level; lvl < newNodeLvl; lvl++ {
			update[lvl] = skiplist.Header
		}
		skiplist.Level = newNodeLvl
	}

	// Create the node and reconnect the data

	insertedNode := newNode(rec, skiplist.Level)

	for lvl := 0; lvl < skiplist.Level; lvl++ {
		insertedNode.Next[lvl] = update[lvl].Next[lvl]
		update[lvl].Next[lvl] = &insertedNode
	}

	skiplist.Count += 1
	return true
}

// Remove marks a node of given key as 'removed'. If the node doesn't exist, nothing happens. Note
// that by "mark as removed" is meant that the Record stored inside the node is modified by marking
// its tombstone. skiplist change only effects the status of the Record instance inside the Skiplist.
func (skiplist *Skiplist) Remove(key []byte) {
	nodeToRemove := skiplist.Find(key, true)
	if nodeToRemove != nil {
		nodeToRemove.Data.Status |= record.RECORD_TOMBSTONE_REMOVED
	}
}

// Find traverses the Skiplist, looking for a node with the given key.
// 	key             Key to search for
//	ignoreDeleted   when true, if a node is found but marked with a tombstone, function returns nil
//	returns         A pointer to the node, or nil if not found
func (skiplist Skiplist) Find(key []byte, ignoreDeleted bool) *SkiplistNode {
	node := skiplist.Header

	// Go from top to bottom level, and find the node with the greatest key less than 'key'.

	for lvl := skiplist.Level - 1; lvl >= 0; lvl-- {
		for node.Next[lvl] != nil && bytes.Compare(key, node.Next[lvl].Data.Key) == 1 {
			node = node.Next[lvl]
		}
	}

	node = node.Next[0]

	if node == nil || bytes.Compare(key, node.Data.Key) != 0 {
		return nil
	}
	if ignoreDeleted && node.Data.IsDeleted() {
		return nil
	}
	return node
}
