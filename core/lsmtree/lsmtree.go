package lsmtree

// TODO: merge() shouldn't return the records. What if there's billions of them? Find a way to use
// the sstable functions that work both for memtables AND for in-place records
// TODO: consider using a real priority queue

import (
	"bufio"
	"bytes"
	"nakevaleng/core/record"
	"nakevaleng/core/skiplist"
	"nakevaleng/core/sstable"
	"nakevaleng/util/filename"
	"os"
	"sort"
)

const (
	// TODO: Make this configurable
	LVL_MAX = 4 // How many levels can the LSM tree have
	RUN_MAX = 2 // How many runs can a level have
)

type recordHandlePair struct {
	Rec    record.Record
	Handle int
}

// needsCompaction checks if the given level in the LSM tree is ready for compaction. A compaction
// should happen whenever the current amount of runs on a signle level exceeds the maximum runs on
// a level configured for the database.
func needsCompaction(path, dbname string, level int) bool {
	return filename.GetLastRun(path, dbname, level) >= RUN_MAX-1
}

// Compact performs a compaction on a whole level in the LSM tree.
// If the level is not ready or unable for compaction, nothing happens.
// Levels which are unable for compaction are: 0, and any level beyond the maximum levels configured
// for the database.
// The result of a compaction is a new SSTable in the first available run on the next level.
// Chaining is performed in case the next level requires a compation after a new SSTable is created.
// Only the Data table is created from the existing set, everything else is recreated.
func Compact(path, dbname string, level int) {
	if !needsCompaction(path, dbname, level) {
		return
	}
	if level >= LVL_MAX {
		return
	}
	if level <= 0 {
		return
	}

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
	resultingDataTable := merge(inFileHandles)

	skipli := skiplist.New(4)
	for _, d := range resultingDataTable {
		skipli.Write(d)
	}
	sstable.MakeTable(path, dbname, outLevel, outRun, &skipli)

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

	Compact(path, dbname, level+1)
}

// Merge performs a k-way merge for the tables on a given level and stores the result in a slice of
// records, which can then be written to a file.
// infile keeps a pointer to file handles for each Data table on a given level, opened for reading.
func merge(infile []*os.File) []record.Record {
	res := []record.Record{}

	// Each file gets a reader. Also, implicitly, each reader is assigned a number.

	readers := []*bufio.Reader{}
	for _, f := range infile {
		rd := bufio.NewReader(f)
		readers = append(readers, rd)
	}

	// Priority queue (implemented as a slice with sorting).

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

		res = append(res, head.Rec)

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

	return res
}
