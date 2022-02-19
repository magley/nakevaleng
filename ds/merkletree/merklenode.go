package merkletree

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"io"
)

const (
	MERKLE_NODE_EMPTY = 1
)

// MerkleNode is a structure for a Merkle tree node.
type MerkleNode struct {
	Data  []byte
	Left  *MerkleNode
	Right *MerkleNode
}

// String returns hexadecimal representation of the node's data.
func (node *MerkleNode) String() string {
	return hex.EncodeToString(node.Data[:])
}

// NewLeaf creates a MerkleNode as a leaf with contents hashed from 'data'.
func NewLeaf(data []byte) MerkleNode {
	h := sha1.Sum(data)
	return MerkleNode{
		Data:  h[:],
		Left:  nil,
		Right: nil,
	}
}

// Serialize appends node data to the specified file.
func (node *MerkleNode) Serialize(writer *bufio.Writer) {
	// Flags

	flags := byte(0)
	if len(node.Data) == 0 {
		flags |= MERKLE_NODE_EMPTY
	}

	// Write flags and data if not empty

	flagsBuffer := make([]byte, 1)
	flagsBuffer[0] = flags

	_, err := writer.Write(flagsBuffer)
	if err != nil {
		panic(err)
	}

	if (flags & MERKLE_NODE_EMPTY) == MERKLE_NODE_EMPTY {
		// Do nothing.
	} else {
		_, err := writer.Write(node.Data)
		if err != nil {
			panic(err)
		}
	}
}

// Deserialize reads data from file reader into this node's Data field.
// Child references are not overwritten!
func (node *MerkleNode) Deserialize(reader *bufio.Reader) bool {
	flagsBuf := make([]byte, 1)
	_, err := io.ReadFull(reader, flagsBuf)

	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return true
		} else {
			panic(err)
		}
	}

	if (flagsBuf[0] & MERKLE_NODE_EMPTY) == MERKLE_NODE_EMPTY {
		node.Data = []byte{}
	} else {
		dataBuf := make([]byte, 20)
		_, err = io.ReadFull(reader, dataBuf)

		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return true
			} else {
				panic(err)
			}
		}

		node.Data = dataBuf[:]
	}
	return false
}

// rehash recalculates the hash data for this node (and all its child nodes).
func (node *MerkleNode) rehash() []byte {
	if node.Left == nil && node.Right == nil {
		return node.Data
	}

	leftHash := node.Left.rehash()
	rightHash := node.Right.rehash()
	hash := sha1.Sum(append(leftHash, rightHash...))
	return hash[:]
}
