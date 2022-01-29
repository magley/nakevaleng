package main

import (
	"bufio"
	"fmt"
	"os"

	"nakevaleng/core/record"
	"nakevaleng/core/skiplist"
	"nakevaleng/core/sstable"
	"nakevaleng/ds/bloomfilter"
	"nakevaleng/ds/cmsketch"
	"nakevaleng/ds/merkletree"
	"nakevaleng/util/filename"
)

func main() {
	// --------------------------------------------------------------------------------------------
	// SSTable

	const (
		dataDir = "data/"
		dbName  = "nakevaleng"
	)

	// Write some data

	skipli := skiplist.New(3)
	skipli.Write(record.NewFromString("Key00", "Val"))
	skipli.Write(record.NewFromString("Key04", "Val"))
	skipli.Write(record.NewFromString("Key02", "Val"))
	skipli.Write(record.NewFromString("Key01", "Val"))
	skipli.Write(record.NewFromString("Key05", "Val"))
	skipli.Write(record.NewFromString("Key03", "Val"))

	lvl := 1
	run := filename.GetLastRun(dataDir, dbName, lvl) + 1

	sstable.MakeTable(dataDir, dbName, lvl, run, &skipli)

	// Test index file

	keyToFind := "Key03"
	byteOffset := sstable.FindIndex(
		filename.Table(dataDir, dbName, lvl, run, filename.TypeIndex),
		[]byte(keyToFind),
		0,
	)

	if byteOffset == -1 {
		fmt.Printf("%s is not in this table\n", keyToFind)
	} else {
		f, _ := os.Open(filename.Table(dataDir, dbName, lvl, run, filename.TypeData))
		defer f.Close()
		r := bufio.NewReader(f)

		f.Seek(byteOffset, 0)
		rec := record.Record{}
		rec.Deserialize(r)
		fmt.Println(rec.ToString())
	}

	// Test summary file

	keyToFind = "Key03"
	summaryByteOffset := sstable.FindSparseIndex(
		filename.Table(dataDir, dbName, lvl, run, filename.TypeSummary),
		[]byte(keyToFind),
	)

	if summaryByteOffset == -1 {
		fmt.Printf("%s is not in this table\n", keyToFind)
	} else {
		byteOffset := sstable.FindIndex(
			filename.Table(dataDir, dbName, lvl, run, filename.TypeIndex),
			[]byte(keyToFind),
			summaryByteOffset,
		)

		if byteOffset == -1 {
			fmt.Printf("%s is not in this table\n", keyToFind)
		} else {
			f, _ := os.Open(filename.Table(dataDir, dbName, lvl, run, filename.TypeData))
			defer f.Close()
			r := bufio.NewReader(f)

			f.Seek(byteOffset, 0)
			rec := record.Record{}
			rec.Deserialize(r)
			fmt.Println(rec.ToString())
		}
	}
}

func main2() {
	//---------------------------------------------------------------------------------------------
	// Filename

	// Create table filename from params

	fname1 := filename.Table("data/", "nakevaleng", 1, 0, filename.TypeData)
	fname2 := filename.Table("data/", "nakevaleng", 1, 0, filename.TypeFilter)
	fname3 := filename.Table("data/", "nakevaleng", 1, 1, filename.TypeSummary)
	fmt.Println(fname1)
	fmt.Println(fname2)
	fmt.Println(fname3)
	os.Create(fname1)
	os.Create(fname2)
	os.Create(fname3)

	// Create next run on level 1

	nextRun := filename.GetLastRun("data/", "nakevaleng", 1) + 1
	nextFnm := filename.Table("data/", "nakevaleng", 1, nextRun, filename.TypeData)
	fmt.Println(nextFnm)
	os.Create(nextFnm)

	// Create next level (level 2)

	nextLvl := filename.GetLastLevel("data/", "nakevaleng") + 1
	nextRun2 := filename.GetLastRun("data/", "nakevaleng", nextLvl) + 1

	nextFnm2 := filename.Table("data/", "nakevaleng", nextLvl, nextRun2, filename.TypeData)
	fmt.Println(nextFnm2)
	os.Create(nextFnm2)

	// Querying

	dbname, lvl, rn, ftype := filename.Query(nextFnm2)
	fmt.Println(dbname == "nakevaleng", lvl == nextLvl, rn == nextRun2, ftype == filename.TypeData)

	// Create log filename from params

	fnamelog1 := filename.Log("data/log/", "nakevaleng", 0)
	fnamelog2 := filename.Log("data/log/", "nakevaleng", 1)
	fmt.Println(fnamelog1)
	fmt.Println(fnamelog2)
	os.Create(fnamelog1)
	os.Create(fnamelog2)

	// Create next log filename

	logNo3 := filename.GetLastLog("data/log/", "nakevaleng") + 1
	fnamelog3 := filename.Log("data/log/", "nakevaleng", logNo3)
	fmt.Println(fnamelog3)
	os.Create(fnamelog3)

	// Querying

	dbname, logno, _, ftype := filename.Query(fnamelog3)
	fmt.Println(dbname == "nakevaleng", logno == logNo3, ftype == filename.TypeLog)

	fmt.Println("\n=================================================\n")

	//---------------------------------------------------------------------------------------------
	// Skiplist

	// Create new

	skiplist := skiplist.New(3)

	{
		// Some data

		r1 := record.NewFromString("Key01", "Val01")
		r2 := record.NewFromString("Key02", "Val05")
		r3 := record.NewFromString("Key03", "Val02")
		r4 := record.NewFromString("Key04", "Val04")

		r1.TypeInfo = 1 // e.g. TypeInfo 1 == CountMinSketch
		r2.TypeInfo = 2 // e.g. TypeInfo 2 == HyperLogLog

		// Insert into skiplist

		skiplist.Write(r1)
		skiplist.Write(r3)
		skiplist.Write(r4)
		skiplist.Write(r2)
	}

	// Key-based find

	fmt.Println("Find Key01...", skiplist.Find([]byte("Key01"), true).Data.ToString())
	fmt.Println("Find Key02...", skiplist.Find([]byte("Key02"), true).Data.ToString())
	fmt.Println("Find Key04...", skiplist.Find([]byte("Key04"), true).Data.ToString())

	// Change with new type

	{
		r4_new := skiplist.Find([]byte("Key04"), true).Data
		r4_new.TypeInfo = 3
		skiplist.Write(r4_new)
	}

	fmt.Println("Find Key04...", skiplist.Find([]byte("Key04"), true).Data.ToString())

	// Remove elements

	skiplist.Remove([]byte("Key05"))
	skiplist.Remove([]byte("Key07")) // Shouldn't do anything since Key07 was not in our skiplist.
	fmt.Println("Find Key05 (removed)...", skiplist.Find([]byte("Key05"), true))
	fmt.Println("Find Key07 (noexist)...", skiplist.Find([]byte("Key05"), true))

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

	// Change type

	rec1.TypeInfo = 5 // Meaningless without context

	// Clone

	rec1_clone := record.Clone(rec1)

	// Print

	fmt.Println("Rec1:", rec1.ToString())
	fmt.Println("Rec2:", rec2.ToString())
	fmt.Println("Rec1 Clone:", rec1_clone.ToString())

	// Check its tombstone

	fmt.Println("Is it deleted:", rec1.IsDeleted()) // Should be false

	// Append to file

	os.Remove("data/record.bin")

	{
		f, _ := os.OpenFile("data/record.bin", os.O_APPEND, 0666)
		defer f.Close()
		w := bufio.NewWriter(f)
		defer w.Flush()

		rec1.Serialize(w)
		rec2.Serialize(w)
	}

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
