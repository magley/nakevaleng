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

	offsetIndex := int64(0)   // Refers to the offset in a Data table, used in an Index table
	offsetSummary := int64(0) // Refers to the offset in an Index table, used in a Summary table
	k := 0                    // How ITEs have been written so far for the current STE
	summaryPageSize := 3      // TODO: Make this configurable

	// Summary Table: write header first, and then the entires. Problem: the header depends on the
	// entries' data. One solution is to do a 2-pass but it results in ugly code. It's actually OK
	// to put all the entries into memory first and dump them to disk later, because Summary tables
	// are meant to be small enough to keep in memory when reading (unlike Index tables).

	summaryHeader := summaryTableHeader{}
	summaryEntries := make([]summaryTableEntry, 0)

	for n := list.Header.Next[0]; n != nil; {
		record := n.Data

		// First node holds min key.

		if n == list.Header.Next[0] {
			summaryHeader.MinKey = record.Key
		}

		// Immediately write the ITE for this record.

		ite := indexTableEntry{KeySize: record.KeySize, Offset: offsetIndex, Key: record.Key}
		ite.Write(wIndex)
		offsetIndex += int64(record.TotalSize())

		// Move to next node and update counter (this is why n can't be updated in the loop decl.)

		n = n.Next[0]
		k += 1

		// For every block of summaryBlockSize-many ITEs, create one STE.

		if k%(summaryPageSize-1) == 0 || n == nil {
			ste := summaryTableEntry{KeySize: ite.KeySize, Offset: offsetSummary, Key: ite.Key}
			summaryEntries = append(summaryEntries, ste)

			offsetSummary += ite.CalcSize()
			summaryHeader.Payload += uint64(ste.CalcSize())

			k = 0
		}

		// Last node holds max key.

		if n == nil {
			summaryHeader.MaxKey = record.Key
		}
	}

	// Write entire summary.

	summaryHeader.MinKeySize = uint64(len(summaryHeader.MinKey))
	summaryHeader.MaxKeySize = uint64(len(summaryHeader.MaxKey))
	summaryHeader.Write(wSummary)
	for _, ste := range summaryEntries {
		ste.Write(wSummary)
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
