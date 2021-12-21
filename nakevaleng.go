package main

import (
	"fmt"
	"nakevaleng/ds/bloomfilter"
	"nakevaleng/ds/merkle_tree"
)

func main() {
	// Create bloom filter.

	bf := bloomfilter.NewBloomFilter(10, 0.2)

	// Insert elements.

	bf.Insert([]byte("KEY00"))
	bf.Insert([]byte("KEY01"))
	bf.Insert([]byte("KEY02"))
	bf.Insert([]byte("KEY03"))
	bf.Insert([]byte("KEY05"))

	// Query elements (true, false).

	fmt.Println(bf.Query([]byte("KEY00")))
	fmt.Println(bf.Query([]byte("KEY04")))

	// Insert and query again (true).

	bf.Insert([]byte("KEY04"))
	fmt.Println(bf.Query([]byte("KEY04")))

	// Serialize & deserialize (true)

	bf.EncodeToFile("filter.db")

	bf2 := bloomfilter.DecodeFromFile("filter.db")
	fmt.Println(bf2.Query([]byte("KEY04")))

	fmt.Println("===========================================")

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

	mt.Serialize("metadata.db")
	mt2 := merkle_tree.MerkleTree{}
	mt2.Deserialize("metadata.db")
	fmt.Println("mt2 root:\t", mt2.Root.ToString())

	// Check for corruption.

	fmt.Println("mt is valid:\t", mt.Validate())
	fmt.Println("mt2 is valid:\t", mt2.Validate())
}
