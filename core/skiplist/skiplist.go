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
}

// New creates an empty Skiplist with height 'level'.
// Throws an error if specified height is greater than the maximium allowed height.
func New(level int) Skiplist {
	lvlMax := 4 // TODO: Make lvlMax configurable (config file)

	if level > lvlMax {
		fmt.Println("ERROR: Maximum skiplist height is", lvlMax, ", but", level, "was given.")
		panic(nil)
	}

	if level <= 0 {
		fmt.Println("ERROR: Minimum skiplist height is 1, but", level, "was given.")
		panic(nil)
	}

	header := NewNode(make([]byte, 0), make([]byte, 0), level)

	return Skiplist{
		Level:    level,
		LevelMax: lvlMax,
		Header:   &header,
	}
}

// Clear removes all nodes from the Skiplist and resets the number of levels to 1.
func (this *Skiplist) Clear() {
	this.Level = 1
	emptyHeader := NewNode(make([]byte, 0), make([]byte, 0), this.LevelMax)
	this.Header = &emptyHeader
}

// Write works as an 'upsert' operation: update node with given key or insert if it doesn't exist.
// 	key	::	Key of the node to insert/modify
// 	val	::	Value of the new node/new value of the node
func (this *Skiplist) Write(key, val []byte) {
	node := this.Header
	update := make([]*SkiplistNode, this.LevelMax) // A ptr to a level-i node that'll get relinked.

	// Go from top to bottom level, and find the node with the greatest key less than 'key'.

	for lvl := this.Level - 1; lvl >= 0; lvl-- {
		for node.Next[lvl] != nil && bytes.Compare(key, node.Next[lvl].Value.Key) == 1 {
			node = node.Next[lvl]
		}
		update[lvl] = node
	}

	node = node.Next[0]

	// Node with given key already exists: update the value and timestamp.

	if !(node == nil || bytes.Compare(key, node.Value.Key) != 0) {
		node.Value = record.New(key, val)
		return
	}

	// Determine how many levels the new node will have.

	newNodeLvl := 1

	rnd := rand.Intn(2)
	for rnd != 0 && newNodeLvl < this.LevelMax {
		newNodeLvl += 1
		rnd = rand.Intn(2)
	}

	// If the list doesn't use newNodeLvl-many levels, we'll make it so that it does.
	// "Bonus" nodes live in the header, because 'update' has nodes that go BEFORE our new node.

	if newNodeLvl > this.Level {
		for lvl := this.Level; lvl < newNodeLvl; lvl++ {
			update[lvl] = this.Header
		}
		this.Level = newNodeLvl
	}

	// Create the node and reconnect the data

	insertedNode := NewNode(key, val, this.Level)

	for lvl := 0; lvl < this.Level; lvl++ {
		insertedNode.Next[lvl] = update[lvl].Next[lvl]
		update[lvl].Next[lvl] = &insertedNode
	}
}

// Find traverses the Skiplist, looking for a node with the given key.
// 	key			Key to search for
//	returns		A pointer to the node, or nil if none found
func (this Skiplist) Find(key []byte) *SkiplistNode {
	node := this.Header

	// Go from top to bottom level, and find the node with the greatest key less than 'key'.

	for lvl := this.Level - 1; lvl >= 0; lvl-- {
		for node.Next[lvl] != nil && bytes.Compare(key, node.Next[lvl].Value.Key) == 1 {
			node = node.Next[lvl]
		}
	}

	node = node.Next[0]

	// The final node is not nil and its key matches the one we're looking for.

	if node == nil || bytes.Compare(key, node.Value.Key) != 0 {
		return nil
	}
	return node
}

// Remove marks a node of given key as 'removed'. If the node doesn't exist, nothing happens. Note
// that by "mark as removed" is meant that the Record stored inside the node is modified by marking
// its tombstone. This change only effects the status of the Record instance inside the Skiplist.
// 	key		Key to mark as removed.
func (this *Skiplist) Remove(key []byte) {
	nodeToRemove := this.Find(key)
	if nodeToRemove != nil {
		nodeToRemove.Value.Status |= record.RECORD_TOMBSTONE_REMOVED
	}
}
