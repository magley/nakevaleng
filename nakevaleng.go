package main

import (
	"fmt"
	"nakevaleng/ds/merkle_tree"
)

func main() {

	// Nodes.

	nodes := []merkle_tree.MerkleNode{
		{Data: []byte("1")},
		{Data: []byte("2")},
		{Data: []byte("3")},
		{Data: []byte("4")},
		{Data: []byte("5")},
		{Data: []byte("6")},
		{Data: []byte("7")},
		//{Data: []byte("8")},
	}

	// Build tree.

	mt := merkle_tree.NewMerkleTree(nodes)
	fmt.Println("mt root:\t", mt.Root.ToString())

	// Serialize & deserialize.

	mt.Serialize("merkle.bin")
	mt2 := merkle_tree.MerkleTree{}
	mt2.Deserialize("merkle.bin")
	fmt.Println("mt2 root:\t", mt2.Root.ToString())

	// Check for corruption.

	fmt.Println("mt is valid:\t", mt.Validate())
	fmt.Println("mt2 is valid:\t", mt2.Validate())
}
