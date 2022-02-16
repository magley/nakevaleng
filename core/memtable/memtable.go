package memtable

import (
	"nakevaleng/core/lsmtree"
	"nakevaleng/core/record"
	"nakevaleng/core/skiplist"
	"nakevaleng/core/sstable"
	"nakevaleng/engine/coreconf"
	"nakevaleng/util/filename"
)

type Memtable struct {
	conf coreconf.CoreConfig
	sl   *skiplist.Skiplist
}

// Returns a pointer to a new Memtable object.
func New(conf coreconf.CoreConfig) *Memtable {
	sl := skiplist.New(conf.SkiplistLevel, conf.SkiplistLevelMax)

	return &Memtable{
		conf: conf,
		sl:   sl,
	}
}

// Add a record to the memtable. Returns whether or not the memtable is now full, making flushing necessary.
// Note that if a record with the same key already exists in the memtable, it gets updated with the new value,
// with no change to the memtable size.
func (mt *Memtable) Add(rec record.Record) bool {
	mt.sl.Write(rec)

	return mt.sl.Count == mt.conf.MemtableCapacity
}

// Remove a record with the given key from the memtable. Note that "removing" just means
// setting the tombstone bit (logical deletion).
func (mt *Memtable) Remove(key []byte) {
	mt.sl.Remove(key)
}

// Find a record with the given key in the memtable.
func (mt *Memtable) Find(key []byte) (record.Record, bool) {
	slNode := mt.sl.Find(key)

	if slNode != nil {
		return slNode.Data, true
	} else {
		return record.Record{}, false
	}
}

// Flush the memtable to disk, forming an SSTable.
func (mt *Memtable) Flush() {
	newRun := filename.GetLastRun(mt.conf.Path, mt.conf.DBName, 1) + 1
	sstable.MakeTable(mt.conf.Path, mt.conf.DBName, mt.conf.SummaryPageSize, 1, newRun, mt.sl)
	mt.sl.Clear()
	lsmtree.Compact(mt.conf.Path, mt.conf.DBName, mt.conf.SummaryPageSize, 1, mt.conf.LsmLvlMax, mt.conf.LsmRunMax)
}
