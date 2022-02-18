package merkletree

import (
	"bufio"
	"crypto/sha1"
	"errors"
	"os"
)

// Structure for a Merkle tree.
type MerkleTree struct {
	Root *MerkleNode
}

// New constructs a new merkle tree from a given slice of nodes.
func New(level []MerkleNode) (*MerkleTree, error) {
	if len(level) == 0 {
		return nil, errors.New("cannot build Merkle Tree from 0 nodes")
	}
	tree := MerkleTree{}
	tree.Root = &tree.build(level)[0]
	return &tree, nil
}

// build is a recursive function for building the next level of nodes.
// Empty nodes are inserted in-place to make the tree semi-complete.
// Empty nodes do not alter the merged hash value of the parent node.
// Returns the newly created level. The very last call will always return just one node - the root.
func (tree *MerkleTree) build(level []MerkleNode) []MerkleNode {
	if len(level)%2 != 0 {
		level = append(level, MerkleNode{Data: []byte{}})
	}

	// Build a parent for each pair of nodes on the current level.

	new_level := make([]MerkleNode, 0)

	for i := 0; i < len(level)-1; i += 2 {
		l := level[i]
		r := level[i+1]

		hash_appended := l.Data
		hash_appended = append(hash_appended, r.Data...)
		hash_val := sha1.Sum(hash_appended)

		node := MerkleNode{
			Data:  hash_val[:],
			Left:  &l,
			Right: &r,
		}
		new_level = append(new_level, node)
	}

	// If only 1 node is on the new level, then that's the root.
	// Otherwise, repeat the process for `new_level`.

	if len(new_level) == 1 {
		return new_level
	} else {
		return tree.build(new_level)
	}
}

// Serialize writes the entire tree to disk using breadth-first traversal.
func (tree *MerkleTree) Serialize(fname string) {
	file, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)

	queue := make([]*MerkleNode, 0)
	queue = append(queue, tree.Root)
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]

		if n.Left != nil {
			queue = append(queue, n.Left)
		}
		if n.Right != nil {
			queue = append(queue, n.Right)
		}

		n.Serialize(writer)
	}

	writer.Flush()
}

// Deserialize builds the tree from a file.
// The file should be genereated by Serialize().
// All old data of the tree is removed.
func (tree *MerkleTree) Deserialize(fname string) {
	nodes := make([]MerkleNode, 0)

	// Load tree into linear array.

	file, err := os.OpenFile(fname, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}
	reader := bufio.NewReader(file)

	for true {
		n := MerkleNode{}
		eof := n.Deserialize(reader)
		if eof {
			break
		}
		nodes = append(nodes, n)
	}

	file.Close()

	// In case nothing was loaded, we're done.

	if len(nodes) == 0 {
		tree.Root = nil
		return
	}

	// We have a slice of nodes now, so we'll build the tree.
	// No need to do any hashing, because the nodes already store their hash.

	queue := make([]MerkleNode, 0)
	i := 0
	tree.Root = &nodes[i]
	i++
	queue = append(queue, *tree.Root)

	for len(queue) != 0 {
		n := queue[0]
		queue = queue[1:]

		// Left child.

		if i >= len(queue) {
			break
		}
		n.Left = &nodes[i]
		queue = append(queue, nodes[i])
		i++

		// Right child.

		if i >= len(queue) {
			break
		}
		n.Right = &nodes[i]
		i++
		queue = append(queue, nodes[i])
	}
}

// Validate recalculates the hash for the root and compares it with the current hash.
// Returns true if data is valid.
// Returns false otherwise (implying data corruption).
func (tree *MerkleTree) Validate() bool {
	h := tree.Root.rehash()

	for i := 0; i < 20; i++ {
		if tree.Root.Data[i] != h[i] {
			return false
		}
	}
	return true
}
