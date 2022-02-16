package memtable

import (
	"nakevaleng/core/lsmtree"
	"nakevaleng/core/record"
	"nakevaleng/core/skiplist"
	"nakevaleng/core/sstable"
	coreconf "nakevaleng/engine/core-config"
	"nakevaleng/util/filename"
)

type Memtable struct {
	conf     coreconf.CoreConfig
	skiplist *skiplist.Skiplist
}

// Returns a pointer to a new Memtable object.
func New(conf coreconf.CoreConfig) *Memtable {
	skiplist := skiplist.New(conf.SkiplistLevel, conf.SkiplistLevelMax)

	return &Memtable{
		conf:     conf,
		skiplist: skiplist,
	}
}

// Adds a record to the memtable. Returns whether or not the memtable is now full, making flushing necessary.
// Note that if a record with the same key already exists in the memtable, it gets updated with the new value,
// with no change to the memtable size.
func (memtable *Memtable) Add(rec record.Record) bool {
	memtable.skiplist.Write(rec)

	return memtable.skiplist.Count == memtable.conf.MemtableCapacity
}

// Remove a record with the given key from the memtable. Note that "removing" just means
// setting the tombstone bit (logical deletion).
func (memtable *Memtable) Remove(key []byte) {
	memtable.skiplist.Remove(key)
}

// Find a record with the given key in the memtable.
func (memtable *Memtable) Find(key []byte) (record.Record, bool) {
	slNode := memtable.skiplist.Find(key)

	if slNode != nil {
		return slNode.Data, true
	} else {
		return record.Record{}, false
	}
}

// Flushes the memtable to disk, forming an SSTable.
func (mt *Memtable) Flush() {
	newRun := filename.GetLastRun(mt.conf.Path, mt.conf.DBName, 1) + 1
	sstable.MakeTable(mt.conf.Path, mt.conf.DBName, mt.conf.SummaryPageSize, 1, newRun, mt.skiplist)
	mt.skiplist.Clear()
	lsmtree.Compact(mt.conf.Path, mt.conf.DBName, mt.conf.SummaryPageSize, 1, mt.conf.LsmLvlMax, mt.conf.LsmRunMax)
}
