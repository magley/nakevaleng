package sstable

import (
	"nakevaleng/core/skiplist"
	"nakevaleng/ds/bloomfilter"
	"nakevaleng/ds/merkletree"
	"nakevaleng/util/filename"

	"bufio"
	"os"
)

func MakeTable(path string, dbname string, level int, run int, list *skiplist.Skiplist) {
	makeDataTable(path, dbname, level, run, list)
	makeIndexAndSummary(path, dbname, level, run, list)
	makeFilter(path, dbname, level, run, list)
	makeMetadata(path, dbname, level, run, list)
}

func makeIndexAndSummary(path string, dbname string, level int, run int, list *skiplist.Skiplist) {
	fnameIndex := filename.Table(path, dbname, level, run, filename.TypeIndex)
	fnameSummary := filename.Table(path, dbname, level, run, filename.TypeSummary)

	fIndex, _ := os.Create(fnameIndex)
	wIndex := bufio.NewWriter(fIndex)

	fSummary, _ := os.Create(fnameSummary)
	wSummary := bufio.NewWriter(fSummary)

	offsetIndex := int64(0)
	offsetSummary := int64(0)
	k := 0
	summaryBlockSize := 3 // TODO: Make this configurable

	// First pass: header of the Summary Table.

	minKey := list.Header.Next[0].Data.Key
	maxKey := make([]byte, 0)
	for n := list.Header.Next[0]; n != nil; n = n.Next[0] {
		if n.Next[0] == nil {
			maxKey = n.Data.Key
		}
	}

	sth := summaryTableHeader{
		MinKeySize: uint64(len(minKey)), MinKey: minKey,
		MaxKeySize: uint64(len(maxKey)), MaxKey: maxKey,
	}

	sth.Write(wSummary)

	// Second pass: Index Table and Summary Table

	for n := list.Header.Next[0]; n != nil; {
		record := n.Data

		ite := indexTableEntry{KeySize: record.KeySize, Offset: offsetIndex, Key: record.Key}
		ite.Write(wIndex)

		offsetIndex += int64(record.TotalSize())
		n = n.Next[0]
		k += 1

		if k%(summaryBlockSize-1) == 0 || n == nil {
			ste := summaryTableEntry{KeySize: ite.KeySize, Offset: offsetSummary, Key: ite.Key}
			ste.Write(wSummary)

			offsetSummary += ite.CalcSize()
			k = 0
		}
	}

	wSummary.Flush()
	fSummary.Close()
	wIndex.Flush()
	fIndex.Close()
}

func makeFilter(path string, dbname string, level int, run int, list *skiplist.Skiplist) {
	elemNo := 0 // TODO: Keep the count inside the skiplist? It doesn't affect performance anyway.
	{
		n := list.Header.Next[0]
		for n != nil {
			elemNo += 1
			n = n.Next[0]
		}
	}

	bf := bloomfilter.New(elemNo, 0.01)
	{
		n := list.Header.Next[0]
		for n != nil {
			bf.Insert(n.Data.Key)
			n = n.Next[0]
		}
	}

	bf.EncodeToFile(filename.Table(path, dbname, level, run, filename.TypeFilter))
}

func makeMetadata(path string, dbname string, level int, run int, list *skiplist.Skiplist) {
	merkleNodes := make([]merkletree.MerkleNode, 0)
	{
		n := list.Header.Next[0]
		for n != nil {
			merkleNodes = append(merkleNodes, merkletree.MerkleNode{Data: n.Data.Value})
			n = n.Next[0]
		}
	}

	merkleTree := merkletree.New(merkleNodes)
	merkleTree.Serialize(filename.Table(path, dbname, level, run, filename.TypeMetadata))
}
