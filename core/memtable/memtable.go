package memtable

import (
	"fmt"
	"nakevaleng/core/lsmtree"
	"nakevaleng/core/record"
	"nakevaleng/core/skiplist"
	"nakevaleng/core/sstable"
	"nakevaleng/engine/coreconf"
	"nakevaleng/util/filename"
)

type Memtable struct {
	conf      coreconf.CoreConfig
	memusage  uint64
	threshold uint64
	sl        *skiplist.Skiplist
}

// Returns a pointer to a new Memtable object.
func New(conf coreconf.CoreConfig) *Memtable {
	sl := skiplist.New(conf.SkiplistLevel, conf.SkiplistLevelMax)

	return &Memtable{
		conf:      conf,
		memusage:  uint64(0),
		threshold: conf.MemtableThresholdBytes(),
		sl:        sl,
	}
}

// Add a record to the memtable.
// Returns whether or not the element is a new element in the memtable. In that case, the memtable
// does not grow in size.
// There is no automatic flushing. Check with ShouldFlush() and invoke the operation with Flush().
func (mt *Memtable) Add(rec record.Record) bool {
	oldRecordSize := uint64(0)
	oldNode, isNewElement := mt.sl.Write(rec)
	if oldNode != nil {
		oldRecordSize = uint64(oldNode.Data.TotalSize())
	}

	newRecordSize := rec.TotalSize()

	// It's uint64, so it's either this or converting
	if newRecordSize > oldRecordSize {
		mt.memusage += newRecordSize - oldRecordSize
	} else {
		mt.memusage += oldRecordSize - newRecordSize
	}

	return isNewElement
}

// ShouldFlush returns true if the memtable is ready to be flushed into an sstable, as determined by
// the currently set MemtableFlushStrategy.
func (mt Memtable) ShouldFlush() bool {
	return (mt.conf.ShouldFlushByCapacity() && mt.sl.Count == mt.conf.MemtableCapacity) ||
		(mt.conf.ShouldFlushByThreshold() && mt.memusage >= mt.threshold)
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
	fmt.Println("[DBG]\t[Memtable] Flushing")
	newRun := filename.GetLastRun(mt.conf.Path, mt.conf.DBName, 1) + 1
	sstable.MakeTable(mt.conf.Path, mt.conf.DBName, mt.conf.SummaryPageSize, 1, newRun, mt.NewIterator())
	mt.sl.Clear()
	mt.memusage = 0
	lsmtree.Compact(mt.conf.Path, mt.conf.DBName, mt.conf.SummaryPageSize, 1, mt.conf.LsmLvlMax, mt.conf.LsmRunMax)
}

// NewIterator returns an iterator to the sorted contents of a Memtable
func (mt *Memtable) NewIterator() record.Iterator {
	var rec record.Record
	n := mt.sl.Header.Next[0]
	return func() (record.Record, bool) {
		if n != nil {
			rec = n.Data
			n = n.Next[0]
			return rec, false
		} else {
			n = mt.sl.Header.Next[0]
			return record.NewEmpty(), true
		}
	}
}
