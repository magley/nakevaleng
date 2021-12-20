package merkle_tree

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"io"
)

const (
	MERKLE_NODE_EMPTY = 1
)

// Struture for a Merkle tree node.
//
type MerkleNode struct {
	Data  []byte
	Left  *MerkleNode
	Right *MerkleNode
}

// Get hexadecimal representation of the node's data.
//
func (this *MerkleNode) ToString() string {
	return hex.EncodeToString(this.Data[:])
}

// Append data to the specified file, with a file writer already open.
//
// Will not flush if flush is set to false.
//
// Format:
//
//  +-------+------+
//  | FLAGS | DATA |
//  +-------+------+
//
//	FLAGS
//		empty :: whether the node is empty
//	DATA
//		data, if FLAGS is set to empty, nothing is written
//
func (this *MerkleNode) Serialize(writer *bufio.Writer, flush bool) {
	if flush {
		defer writer.Flush()
	}

	// Flags

	flags := byte(0)
	if len(this.Data) == 0 {
		flags |= MERKLE_NODE_EMPTY
	}

	// Write flags and data if not empty

	flags_buffer := make([]byte, 1)
	flags_buffer[0] = flags

	_, err := writer.Write(flags_buffer)
	if err != nil {
		panic(err)
	}

	if (flags & MERKLE_NODE_EMPTY) == MERKLE_NODE_EMPTY {

	} else {
		_, err := writer.Write(this.Data)
		if err != nil {
			panic(err)
		}
	}
}

// Read data from file reader into this node's Data field.
//
// Child references are not overwritten!
//
func (this *MerkleNode) Deserialize(reader *bufio.Reader) bool {
	flags_buf := make([]byte, 1)
	_, err := io.ReadFull(reader, flags_buf)

	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return true
		} else {
			panic(err)
		}
	}

	if (flags_buf[0] & MERKLE_NODE_EMPTY) == MERKLE_NODE_EMPTY {
		this.Data = []byte{}
	} else {
		data_buf := make([]byte, 20)
		_, err = io.ReadFull(reader, data_buf)

		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return true
			} else {
				panic(err)
			}
		}

		this.Data = data_buf[:]
	}
	return false
}

// Recursively recalculates the hash data for this node.
//
func (this *MerkleNode) rehash() []byte {
	if this.Left == nil && this.Right == nil {
		return this.Data
	}

	lefthash := this.Left.rehash()
	righthash := this.Right.rehash()
	hash := sha1.Sum(append(lefthash, righthash...))
	return hash[:]
}
