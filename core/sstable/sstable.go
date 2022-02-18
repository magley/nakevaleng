package sstable

import (
	"nakevaleng/core/record"
	"nakevaleng/core/skiplist"
	"nakevaleng/ds/bloomfilter"
	"nakevaleng/ds/merkletree"
	"nakevaleng/util/filename"

	"bufio"
	"os"
)

// MakeTable creates a new SSTable from the data given as a skiplist.
// You should only use this when flushing a Memtable to a level 1 SStable (minor compaction).
// For all other cases (i.e. when major compaction happens), use MakeTableSecondaries().
func MakeTable(path, dbname string, summaryPageSize, level, run int, list *skiplist.Skiplist) {
	makeDataTable(path, dbname, level, run, list)
	{
		keycontexts := []record.KeyContext{}
		for n := list.Header.Next[0]; n != nil; n = n.Next[0] {
			keycontexts = append(keycontexts, record.KeyContext{
				Key:     n.Data.Key,
				RecSize: n.Data.TotalSize(),
			})
		}
		makeIndexAndSummary(path, dbname, summaryPageSize, level, run, keycontexts)
		makeFilter(path, dbname, level, run, keycontexts)
	}

	makeMetadata(path, dbname, level, run, list)
}

// MakeTableSecondaries creates a new SSTable (except for the Data table) based on input parameters.
func MakeTableSecondaries(path, dbname string, summaryPageSize, level, run int, merkleleaves []merkletree.MerkleNode, keyctx []record.KeyContext) {
	makeIndexAndSummary(path, dbname, summaryPageSize, level, run, keyctx)
	makeFilter(path, dbname, level, run, keyctx)

	// MakeTableSecondaries and makeMetadata are the only places where merkletree New is called, so I guess panic here?
	// In any case, old behaviour was to panic as well (but in merkletree build)
	merkleTree, err := merkletree.New(merkleleaves)
	if err != nil {
		panic(err)
	}

	merkleTree.Serialize(filename.Table(path, dbname, level, run, filename.TypeMetadata))
}

func makeFilter(path, dbname string, level, run int, keyctx []record.KeyContext) {
	bf, _ := bloomfilter.New(len(keyctx), 0.01)
	for _, kc := range keyctx {
		bf.Insert(kc.Key)
	}

	bf.EncodeToFile(filename.Table(path, dbname, level, run, filename.TypeFilter))
}

func makeMetadata(path, dbname string, level, run int, list *skiplist.Skiplist) {
	merkleNodes := make([]merkletree.MerkleNode, 0)
	{
		n := list.Header.Next[0]
		for n != nil {
			merkleNodes = append(merkleNodes, merkletree.NewLeaf(n.Data.Value))
			n = n.Next[0]
		}
	}

	// MakeTableSecondaries and makeMetadata are the only places where merkletree New is called, so I guess panic here?
	// In any case, old behaviour was to panic as well (but in merkletree build)
	merkleTree, err := merkletree.New(merkleNodes)
	if err != nil {
		panic(err)
	}

	merkleTree.Serialize(filename.Table(path, dbname, level, run, filename.TypeMetadata))
}

func makeIndexAndSummary(path, dbname string, summaryPageSize, level, run int, keyctx []record.KeyContext) {
	fnameIndex := filename.Table(path, dbname, level, run, filename.TypeIndex)
	fnameSummary := filename.Table(path, dbname, level, run, filename.TypeSummary)

	fIndex, _ := os.Create(fnameIndex)
	wIndex := bufio.NewWriter(fIndex)

	fSummary, _ := os.Create(fnameSummary)
	wSummary := bufio.NewWriter(fSummary)

	offsetIndex := int64(0)   // Refers to the offset in a Data table, used in an Index table
	offsetSummary := int64(0) // Refers to the offset in an Index table, used in a Summary table
	// Summary Table: write header first, and then the entires. Problem: the header depends on the
	// entries' data. One solution is to do a 2-pass but it results in ugly code. It's actually OK
	// to put all the entries into memory first and dump them to disk later, because Summary tables
	// are meant to be small enough to keep in memory when reading (unlike Index tables).

	summaryHeader := summaryTableHeader{}
	summaryEntries := make([]summaryTableEntry, 0)

	for i, kc := range keyctx {
		// First entry holds min key.

		if i == 0 {
			summaryHeader.MinKey = kc.Key
		}

		// Write the ITE for this record.

		ite := indexTableEntry{KeySize: uint64(len(kc.Key)), Key: kc.Key, Offset: offsetIndex}
		ite.Write(wIndex)
		offsetIndex += int64(kc.RecSize)

		// Create an STE if we've written k ITE's OR this is the last entry in the slice.

		if (i != 0 && i%(summaryPageSize) == 0) || i == len(keyctx)-1 || i == 0 {
			ste := summaryTableEntry{KeySize: ite.KeySize, Offset: offsetSummary, Key: ite.Key}
			summaryEntries = append(summaryEntries, ste)

			summaryHeader.Payload += uint64(ste.CalcSize())
		}
		offsetSummary += ite.CalcSize()

		// Last entry holds max key.

		if i == len(keyctx)-1 {
			summaryHeader.MaxKey = kc.Key
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
