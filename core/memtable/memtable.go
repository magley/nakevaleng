package memtable

import (
	"fmt"
	"nakevaleng/core/lsmtree"
	"nakevaleng/core/record"
	"nakevaleng/core/skiplist"
	"nakevaleng/core/sstable"
	"nakevaleng/util/filename"
)

type Memtable struct {
	capacity int
	skiplist skiplist.Skiplist
}

// Returns a pointer to a new Memtable object.
func New(capacity, slLevel, slLevelMax int) *Memtable {
	if capacity < 0 {
		errMsg := fmt.Sprint("capacity must be a positive number, but ", capacity, " was given.")
		panic(errMsg)
	}

	skiplist := skiplist.New(slLevel, slLevelMax)

	return &Memtable{
		capacity: capacity,
		skiplist: skiplist,
	}
}

// Adds a record to the memtable. Returns whether or not the memtable is now full, making flushing necessary.
// Note that if a record with the same key already exists in the memtable, it gets updated with the new value,
// with no change to the memtable size.
func (memtable *Memtable) Add(rec record.Record) bool {
	memtable.skiplist.Write(rec)

	return memtable.skiplist.Count == memtable.capacity
}

// Remove a record with the given key from the memtable. Note that "removing" just means
// setting the tombstone bit (logical deletion).
func (memtable *Memtable) Remove(key string) {
	memtable.skiplist.Remove([]byte(key))
}

// Find a record with the given key in the memtable. Note that records that have the tombstone bit
// set are not possible candidates for returning.
func (memtable *Memtable) Find(key string) (record.Record, bool) {
	slNode := memtable.skiplist.Find([]byte(key), true)

	if slNode != nil {
		return slNode.Data, true
	} else {
		return record.Record{}, false
	}
}

// Flushes the memtable to disk, forming an SSTable.
func (memtable *Memtable) Flush(path, dbname string, summaryPageSize, lsmLvlMax, lsmRunMax int) {
	newRun := filename.GetLastRun(path, dbname, 1) + 1
	sstable.MakeTable(path, dbname, summaryPageSize, 1, newRun, &memtable.skiplist)
	memtable.skiplist.Clear()
	lsmtree.Compact(path, dbname, summaryPageSize, 1, lsmLvlMax, lsmRunMax)
}
