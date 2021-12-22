package main

import (
	"fmt"
	"nakevaleng/ds/cmsketch"
	"nakevaleng/ds/merkle_tree"
)

func main() {
	// Create new count-min sketch

	cms := cmsketch.New(0.1, 0.1)

	// Insert

	cms.Insert([]byte("blue"))
	cms.Insert([]byte("blue"))
	cms.Insert([]byte("red"))
	cms.Insert([]byte("green"))
	cms.Insert([]byte("blue"))

	// Query

	fmt.Println("Querying a CMS built in memory, should be: 3, 1, 1, 0, 0")
	fmt.Println(cms.Query([]byte("blue")))
	fmt.Println(cms.Query([]byte("red")))
	fmt.Println(cms.Query([]byte("green")))
	fmt.Println(cms.Query([]byte("yellow")))
	fmt.Println(cms.Query([]byte("orange")))

	// Serialize

	cms.EncodeToFile("cms.bin")
	cms2 := cmsketch.DecodeFromFile("cms.bin")

	fmt.Println("Querying a CMS built from disk, should be: 3, 1, 1, 0, 0")
	fmt.Println(cms2.Query([]byte("blue")))
	fmt.Println(cms2.Query([]byte("red")))
	fmt.Println(cms2.Query([]byte("green")))
	fmt.Println(cms2.Query([]byte("yellow")))
	fmt.Println(cms2.Query([]byte("orange")))

	fmt.Println("=================================================")

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
