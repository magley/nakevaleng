package main

import (
	"bufio"
	"fmt"
	"os"

	"nakevaleng/core/record"
	"nakevaleng/core/skiplist"
	"nakevaleng/ds/bloomfilter"
	"nakevaleng/ds/cmsketch"
	"nakevaleng/ds/merkletree"
)

func main() {
	fmt.Println("\n=================================================\n")

	//---------------------------------------------------------------------------------------------
	// Skiplist

	// Create new

	skiplist := skiplist.New(3)

	// Insert various key-value data

	skiplist.WriteKeyVal([]byte("Key01"), []byte("Val01"))
	skiplist.WriteKeyVal([]byte("Key02"), []byte("Val02"))
	skiplist.WriteKeyVal([]byte("Key04"), []byte("Val04"))
	skiplist.WriteKeyVal([]byte("Key05"), []byte("Val05"))
	skiplist.WriteKeyVal([]byte("Key03"), []byte("Val03"))
	skiplist.WriteKeyVal([]byte("Key06"), []byte("Val06"))

	// Key-based find

	skiplist.Find([]byte("Key01"), true).Data.Status |= record.RECORD_COUNTMINSKETCH
	fmt.Println("Find Key01...", skiplist.Find([]byte("Key01"), true).Data.ToString())
	fmt.Println("Find Key02...", skiplist.Find([]byte("Key02"), true).Data.ToString())
	fmt.Println("Find Key03...", skiplist.Find([]byte("Key03"), true).Data.ToString())

	// Remove elements

	skiplist.Remove([]byte("Key05"))
	skiplist.Remove([]byte("Key07")) // Shouldn't do anything since Key07 was not in our skiplist.

	fmt.Println("Find Key05 (removed)...", skiplist.Find([]byte("Key05"), true))

	// Update elements

	skiplist.WriteKeyVal([]byte("Key01"), []byte("Key01 ***UPDATED***"))
	fmt.Println("Find Key01...", skiplist.Find([]byte("Key01"), true).Data.ToString())

	// Iterate through all nodes

	fmt.Println("All the nodes:")
	{
		n := skiplist.Header.Next[0]
		for n != nil {
			fmt.Println(n.Data.ToString())
			n = n.Next[0]
		}
	}

	// Clear the list

	skiplist.Clear()
	fmt.Println("All the nodes after clearing the list:")
	{
		n := skiplist.Header.Next[0]
		for n != nil {
			fmt.Println(n.Data.ToString())
			n = n.Next[0]
		}
	}

	fmt.Println("\n=================================================\n")

	//---------------------------------------------------------------------------------------------
	// Record

	// Create new

	rec1 := record.NewFromString("Key01", "Val01")
	rec2 := record.NewFromString("Key02", "Val02")

	// Print

	fmt.Println("Rec1:", rec1.ToString())
	fmt.Println("Rec2:", rec2.ToString())

	// Check its tombstone

	fmt.Println("Is it deleted:", rec1.IsDeleted()) // Should be false

	// Append to file

	os.Remove("data/record.bin")

	rec1.Serialize("data/record.bin")
	rec2.Serialize("data/record.bin")

	// Read from file

	rec1_from_file := record.NewEmpty()
	rec2_from_file := record.NewEmpty()

	{
		f, _ := os.OpenFile("data/record.bin", os.O_RDONLY, 0666)
		defer f.Close()
		w := bufio.NewReader(f)

		rec1_from_file.Deserialize(w) // Should equal rec1
		rec2_from_file.Deserialize(w) // Should equal rec2
	}

	fmt.Println("Rec1:", rec1_from_file.ToString())
	fmt.Println("Rec2:", rec2_from_file.ToString())

	fmt.Println("\n=================================================\n")

	//---------------------------------------------------------------------------------------------
	// Count-Min Sketch

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

	cms.EncodeToFile("data/cms.bin")
	cms2 := cmsketch.DecodeFromFile("data/cms.bin")

	fmt.Println("Querying a CMS built from disk, should be: 3, 1, 1, 0, 0")
	fmt.Println(cms2.Query([]byte("blue")))
	fmt.Println(cms2.Query([]byte("red")))
	fmt.Println(cms2.Query([]byte("green")))
	fmt.Println(cms2.Query([]byte("yellow")))
	fmt.Println(cms2.Query([]byte("orange")))

	fmt.Println("\n=================================================\n")

	//---------------------------------------------------------------------------------------------
	// Bloom Filter.

	// Create bloom filter.

	bf := bloomfilter.New(10, 0.2)

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

	bf.EncodeToFile("data/filter.db")

	bf2 := bloomfilter.DecodeFromFile("data/filter.db")
	fmt.Println(bf2.Query([]byte("KEY04")))

	fmt.Println("\n=================================================\n")

	//---------------------------------------------------------------------------------------------
	// Merkle Tree.

	// Nodes.

	nodes := []merkletree.MerkleNode{
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

	mt := merkletree.New(nodes)
	fmt.Println("mt root:\t", mt.Root.ToString())

	// Serialize & deserialize.

	mt.Serialize("data/metadata.db")
	mt2 := merkletree.MerkleTree{}
	mt2.Deserialize("data/metadata.db")
	fmt.Println("mt2 root:\t", mt2.Root.ToString())

	// Check for corruption.

	fmt.Println("mt is valid:\t", mt.Validate())
	fmt.Println("mt2 is valid:\t", mt2.Validate())

	fmt.Println("\n=================================================\n")
}
