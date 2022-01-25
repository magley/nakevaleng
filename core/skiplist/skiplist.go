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
// 	level	Number of available levels in range [0, level).
// Throws an error if specified height is greater than the maximium allowed height or less than 1.
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

	header := newNodeEmpty(lvlMax)

	return Skiplist{
		Level:    level,
		LevelMax: lvlMax,
		Header:   &header,
	}
}

// Clear() removes all nodes from the Skiplist and resets the number of levels to 1.
func (this *Skiplist) Clear() {
	this.Level = 1
	emptyHeader := newNodeEmpty(this.LevelMax)
	this.Header = &emptyHeader
}

// Write() writes a node with the given Record object into the skiplist. If the same key exists, the
// data inside the node is updated.
//	rec		Record object to store inside the node.
func (this *Skiplist) Write(rec record.Record) {
	node := this.Header
	update := make([]*SkiplistNode, this.LevelMax) // A ptr to a level-i node that'll get relinked.

	// Go from top to bottom level, and find the node with the greatest key less than 'key'.

	for lvl := this.Level - 1; lvl >= 0; lvl-- {
		for node.Next[lvl] != nil && bytes.Compare(rec.Key, node.Next[lvl].Data.Key) == 1 {
			node = node.Next[lvl]
		}
		update[lvl] = node
	}

	node = node.Next[0]

	// Node with given key already exists.

	if !(node == nil || bytes.Compare(rec.Key, node.Data.Key) != 0) {
		node.Data = record.Clone(rec)
		return
	}

	// Determine how many levels the new node will have.

	newNodeLvl := 1

	rnd := rand.Intn(2)
	for rnd != 0 && newNodeLvl < this.LevelMax {
		newNodeLvl += 1
		rnd = rand.Intn(2)
	}

	if newNodeLvl > this.LevelMax {
		newNodeLvl = this.LevelMax
	}

	// If the list doesn't use newNodeLvl-many levels, we'll make it so that it does.
	// "Bonus" nodes live in the header, because 'update' stores nodes that go before our new node.

	if newNodeLvl > this.Level {
		for lvl := this.Level; lvl < newNodeLvl; lvl++ {
			update[lvl] = this.Header
		}
		this.Level = newNodeLvl
	}

	// Create the node and reconnect the data

	insertedNode := newNode(rec, this.Level)

	for lvl := 0; lvl < this.Level; lvl++ {
		insertedNode.Next[lvl] = update[lvl].Next[lvl]
		update[lvl].Next[lvl] = &insertedNode
	}
}

// Remove() marks a node of given key as 'removed'. If the node doesn't exist, nothing happens. Note
// that by "mark as removed" is meant that the Record stored inside the node is modified by marking
// its tombstone. This change only effects the status of the Record instance inside the Skiplist.
// 	key		Key to mark as removed.
func (this *Skiplist) Remove(key []byte) {
	nodeToRemove := this.Find(key, true)
	if nodeToRemove != nil {
		nodeToRemove.Data.Status |= record.RECORD_TOMBSTONE_REMOVED
	}
}

// Find traverses the Skiplist, looking for a node with the given key.
// 	key             Key to search for
//	ignoreDeleted   when true, if a node is found but marked with a tombstone, function returns nil
//	returns         A pointer to the node, or nil if not found
func (this Skiplist) Find(key []byte, ignoreDeleted bool) *SkiplistNode {
	node := this.Header

	// Go from top to bottom level, and find the node with the greatest key less than 'key'.

	for lvl := this.Level - 1; lvl >= 0; lvl-- {
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
