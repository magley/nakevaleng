package lsmtree

import (
	"bufio"
	"bytes"
	"fmt"
	"nakevaleng/core/record"
	"nakevaleng/core/sstable"
	"nakevaleng/ds/merkletree"
	"nakevaleng/util/filename"
	"os"
	"sort"
)

// A recordHandlePair stores a record with minimal info regarding the file where the record is in.
type recordHandlePair struct {
	Rec    record.Record // The record object.
	Handle int           // Index for a file. Meaningless without context.
}

// needsCompaction checks if the given level in the LSM tree is ready for compaction. A compaction
// should happen whenever the current amount of runs on a single level exceeds the maximum runs on
// a level configured for the database.
func needsCompaction(path, dbname string, level int, RUN_MAX int) bool {
	return filename.GetLastRun(path, dbname, level) >= RUN_MAX-1
}

// Compact performs a compaction on a whole level in the LSM tree.
// If the level is not ready or unable for compaction, nothing happens.
// Levels which are unable for compaction are: 0, and any level beyond the maximum levels configured
// for the database.
// The result of a compaction is a new SSTable in the first available run on the next level.
// Chaining is performed in case the next level requires a compation after a new SSTable is created.
// Only the Data table is created from the existing set, everything else is recreated.
func Compact(path, dbname string, summaryPageSize int, level int, LVL_MAX, RUN_MAX int) {
	if !needsCompaction(path, dbname, level, RUN_MAX) {
		return
	}
	if level >= LVL_MAX {
		return
	}
	if level <= 0 {
		return
	}

	fmt.Println("[DBG]\t[LSM] Compaction lvl", level)

	// Get all data tables for this level.

	filenames := []string{}
	for i := 0; i <= filename.GetLastRun(path, dbname, level); i++ {
		filenames = append(filenames, filename.Table(path, dbname, level, i, filename.TypeData))
	}

	// File handles.

	inFileHandles := []*os.File{}
	for _, s := range filenames {
		fhandle, err := os.Open(s)

		if err != nil {
			panic(err)
		}

		inFileHandles = append(inFileHandles, fhandle)
	}

	// Create new SSTable.

	outLevel := level + 1
	outRun := filename.GetLastRun(path, dbname, outLevel) + 1
	outDataFname := filename.Table(path, dbname, outLevel, outRun, filename.TypeData)
	merkletreeLeaves, keyCtx := merge(inFileHandles, outDataFname)
	sstable.MakeTableSecondaries(path, dbname, summaryPageSize, outLevel, outRun, merkletreeLeaves, keyCtx)

	// Close everything and remove tables from the old level.

	for _, f := range inFileHandles {
		f.Close()
	}
	for i := 0; i <= filename.GetLastRun(path, dbname, level); i++ {
		os.Remove(filename.Table(path, dbname, level, i, filename.TypeData))
		os.Remove(filename.Table(path, dbname, level, i, filename.TypeFilter))
		os.Remove(filename.Table(path, dbname, level, i, filename.TypeIndex))
		os.Remove(filename.Table(path, dbname, level, i, filename.TypeSummary))
		os.Remove(filename.Table(path, dbname, level, i, filename.TypeMetadata))
	}

	// Chaining (won't do anything if next level doesn't need compaction yet).

	Compact(path, dbname, summaryPageSize, level+1, LVL_MAX, RUN_MAX)
}

// merge performs a k-way merge for the tables on a given level.
// the Data table is written on disk immediately.
// All secondary files are left to the caller to create, using what's returned by this function.
// infile keeps a pointer to file handles for each Data table on a given level, opened for reading.
// outDataFname is the filename of the resulting Data table that gets created.
// Function returns a list of leaves for the corresponding Merkle tree and a list of KeyContext-s
// from which everything else (bloom filter, index table, summary table) can be built.
func merge(infile []*os.File, outDataFname string) ([]merkletree.MerkleNode, []record.KeyContext) {
	f, err := os.Create(outDataFname)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()

	mtleaves := []merkletree.MerkleNode{}
	keyctx := []record.KeyContext{}

	// Each input file gets a reader. Also, implicitly, each reader is assigned a number.

	readers := []*bufio.Reader{}
	for _, f := range infile {
		rd := bufio.NewReader(f)
		readers = append(readers, rd)
	}

	// Priority queue (implemented as a slice with sorting). TODO: Use a heap.

	pq := []recordHandlePair{}

	for hID, rd := range readers {
		rec := record.Record{}
		eof := rec.Deserialize(rd)
		if !eof {
			pq = append(pq, recordHandlePair{Rec: rec, Handle: hID})
		}
	}

	// Merge until the priority queue is exhausted.

	for len(pq) > 0 {
		// Sort the priority queue by key. If keys are the same, order by timestamp.

		sort.Slice(pq, func(i, j int) bool {
			keyCmp := bytes.Compare(pq[i].Rec.Key, pq[j].Rec.Key)
			biggerTimeStamp := pq[i].Rec.Timestamp > pq[j].Rec.Timestamp
			return keyCmp < 0 || (keyCmp == 0 && biggerTimeStamp)
		})

		// Keep track of which files have had an entry in the priority queue taken away from.
		// Normally it would be just the one whose element has highest priority, but we also do
		// conflict resolution. Each element removed from the pq requires an insertion of the next
		// element from the corresponding file.

		rmvd := []bool{}
		for range readers {
			rmvd = append(rmvd, false)
		}

		// Get element with highest priority.

		head := pq[0]
		pq = pq[0:]
		rmvd[head.Handle] = true

		// Conflict resolution - remove elements with same key and smaller timestamp.

		pqTemp := []recordHandlePair{}
		for _, elem := range pq {
			if bytes.Compare(elem.Rec.Key, head.Rec.Key) != 0 {
				pqTemp = append(pqTemp, elem)
			} else {
				rmvd[elem.Handle] = true
			}
		}
		pq = pqTemp

		// Write element to new SSTable.

		head.Rec.Serialize(w)
		mtleaves = append(mtleaves, merkletree.NewLeaf(head.Rec.Value))
		keyctx = append(keyctx, record.KeyContext{
			Key:     head.Rec.Key,
			RecSize: head.Rec.TotalSize(),
		})

		// Fetch next element for all files that require it (if the reader isn't at EOF).

		for hID, rd := range readers {
			if rmvd[hID] {
				rec := record.Record{}
				eof := rec.Deserialize(rd)
				if !eof {
					pq = append(pq, recordHandlePair{Rec: rec, Handle: hID})
				}
			}
		}
	}

	return mtleaves, keyctx
}
