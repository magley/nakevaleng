package coreeng

import (
	"bufio"
	"fmt"
	"nakevaleng/core/wal"
	coreconf "nakevaleng/engine/core-config"
	"os"
	"time"

	"nakevaleng/core/lru"
	"nakevaleng/core/lsmtree"
	"nakevaleng/core/record"
	"nakevaleng/core/skiplist"
	"nakevaleng/core/sstable"
	"nakevaleng/ds/bloomfilter"
	"nakevaleng/ds/tokenbucket"
	"nakevaleng/util/filename"
)

type CoreEngine struct {
	conf  coreconf.CoreConfig
	cache lru.LRU
	sl    skiplist.Skiplist
	wal   wal.WAL
}

func New(conf coreconf.CoreConfig) *CoreEngine {
	// todo remember to check others when implementing config here!!!
	if len(conf.InternalStart) == 0 {
		return nil
	}
	return &CoreEngine{
		conf,
		*lru.New(conf.CacheCapacity),
		skiplist.New(conf.SkiplistLevel, conf.SkiplistLevelMax),
		*wal.New(conf.WalPath, conf.DBName, conf.WalMaxRecsInSeg,
			conf.WalLwmIdx, conf.WalBufferCapacity),
	}
}

// IsLegal returns true if legal key, otherwise false
func (cen CoreEngine) IsLegal(key []byte) bool {
	start := []byte(cen.conf.InternalStart)
	count := 0
	for i, c := range start {
		if key[i] == c {
			count += 1
		}
	}
	if count == len(start) {
		return false
	}
	return true
}

func (cen CoreEngine) Get(user, key []byte) (record.Record, bool) {
	legal := cen.IsLegal(key)
	if !legal {
		// todo might want to handle this somewhere else
		fmt.Println("ILLEGAL QUERY:", key)
		return record.Record{}, false
	}
	tb := cen.getTokenBucket(user)
	for !tb.HasEnoughTokens() {
		fmt.Println("Slow down!")
		time.Sleep(1 * time.Second)
	}
	cen.putTokenBucket(user, tb)
	return cen.get(key)
}

// Get without checking legality or getting token buckets
func (cen CoreEngine) get(key []byte) (record.Record, bool) {
	// Memtable, sort of

	n := cen.sl.Find(key, false)
	if n != nil {
		nRec := n.Data
		cen.cache.Set(nRec)
		//fmt.Println("[found in skiplist]", nRec)
		if nRec.IsDeleted() {
			return record.Record{}, false
		}
		return nRec, true
	}

	// Cache

	r, foundInCache := cen.cache.Get(string(key))
	if foundInCache {
		cen.cache.Set(r)
		//fmt.Println("[cache hit]", r)
		if r.IsDeleted() {
			return record.Record{}, false
		}
		return r, true
	}

	// Disk

	greatestLevel := filename.GetLastLevel(cen.conf.Path, cen.conf.DBName)

	for j := 1; j <= greatestLevel; j++ {
		greatestRun := filename.GetLastRun(cen.conf.Path, cen.conf.DBName, j)

		for i := greatestRun; i >= 0; i-- {
			// Filter

			q := bloomfilter.
				DecodeFromFile(filename.Table(cen.conf.Path, cen.conf.DBName, j, i, filename.TypeFilter)).
				Query(key)

			if !q {
				//fmt.Printf("%s Not found @ [FILTER] @ L%d R%d\n", key, j, i)
				continue
			}

			// Summary

			ste := sstable.FindSummaryTableEntry(
				filename.Table(cen.conf.Path, cen.conf.DBName, j, i, filename.TypeSummary),
				key,
			)

			//fmt.Println("AA", ste.Offset, key, j, i)

			if ste.Offset == -1 {
				//fmt.Printf("%s Not found @ [SUMMARY] @ L%d R%d\n", key, j, i)
				continue
			}

			// Index

			ite := sstable.FindIndexTableEntry(
				filename.Table(cen.conf.Path, cen.conf.DBName, j, i, filename.TypeIndex),
				key,
				ste.Offset,
			)

			//fmt.Println("BB", ite.Offset, key)

			if ite.Offset == -1 {
				//fmt.Printf("%s Not found @ [INDEX] @ L%d R%d\n", key, j, i)
				continue
			}

			// Data

			f, _ := os.Open(filename.Table(cen.conf.Path, cen.conf.DBName, j, i, filename.TypeData))
			r := bufio.NewReader(f)

			f.Seek(ite.Offset, 0)
			rec := record.Record{}
			rec.Deserialize(r)
			f.Close()

			// record is deleted, so don't return it
			if rec.IsDeleted() {
				//fmt.Println("[RESPECTING THE DEAD]", rec)
				return record.Record{}, false
			}

			// todo should this be a few lines above for consistency?
			cen.cache.Set(rec)
			return rec, true
		}
	}

	return record.Record{}, false
}

func (cen CoreEngine) getTokenBucket(user []byte) tokenbucket.TokenBucket {
	tbKey := []byte(cen.conf.InternalStart)
	tbKey = append(tbKey, user...)
	tbRec, found := cen.get(tbKey)
	if !found {
		return *tokenbucket.New(cen.conf.TokenBucketTokens, cen.conf.TokenBucketInterval)
	}
	return tokenbucket.FromBytes(tbRec.Value)
}

func (cen CoreEngine) putTokenBucket(user []byte, bucket tokenbucket.TokenBucket) {
	tbKey := []byte(cen.conf.InternalStart)
	tbKey = append(tbKey, user...)
	cen.put(record.New(tbKey, bucket.ToBytes()))
}

func (cen CoreEngine) Put(user, key, val []byte, typeInfo byte) bool {
	legal := cen.IsLegal(key)
	if !legal {
		// todo might want to handle this somewhere else by returning err
		fmt.Println("ILLEGAL QUERY:", key)
		return false
	}
	tb := cen.getTokenBucket(user)
	for !tb.HasEnoughTokens() {
		fmt.Println("Slow down!")
		time.Sleep(1 * time.Second)
	}
	cen.putTokenBucket(user, tb)
	rec := record.New(key, val)
	rec.TypeInfo = typeInfo
	cen.put(rec)
	return true
}

func (cen CoreEngine) put(rec record.Record) {
	// assume only TokenBuckets can be illegal for now, todo might want to change to TypeInfo
	isTokenBucket := !cen.IsLegal(rec.Key)
	if !isTokenBucket {
		cen.wal.BufferedAppend(rec)
	}
	cen.cache.Set(rec)
	cen.sl.Write(rec)

	if cen.sl.Count > cen.conf.MemtableCapacity {
		newRun := filename.GetLastRun(cen.conf.Path, cen.conf.DBName, 1) + 1
		sstable.MakeTable(cen.conf.Path, cen.conf.DBName, cen.conf.SummaryPageSize, 1, newRun, &cen.sl)
		cen.sl.Clear()
		lsmtree.Compact(cen.conf.Path, cen.conf.DBName, cen.conf.SummaryPageSize, 1, cen.conf.LsmLvlMax, cen.conf.LsmRunMax)
		// safe to delete old segments now since everything is on disk
		cen.wal.DeleteOldSegments()
	}
}

func (cen CoreEngine) Delete(user, key []byte) bool {
	legal := cen.IsLegal(key)
	if !legal {
		// todo might want to handle this somewhere else by returning err
		fmt.Println("ILLEGAL QUERY:", key)
		return false
	}
	tb := cen.getTokenBucket(user)
	for !tb.HasEnoughTokens() {
		fmt.Println("Slow down!")
		time.Sleep(1 * time.Second)
	}
	cen.putTokenBucket(user, tb)
	rec, found := cen.get(key)
	if !found {
		fmt.Println("CAN'T DELETE. NO SUCH RECORD WITH KEY:", key)
		return false
	}
	// todo remove two below
	if rec.IsDeleted() {
		panic(rec)
	}
	rec.Status |= record.RECORD_TOMBSTONE_REMOVED
	cen.put(rec)
	// todo maybe add cache removal here
	return true
}

func main() {
	engine := *New(coreconf.LoadConfig("conf.yaml"))
	test(engine)
}

func test(engine CoreEngine) {
	user := "USER"
	noType := byte(0) // doesn't matter for now, the wrapper engine should bother with it

	// Insert

	for i := 0; i < 200; i++ {
		engine.Put([]byte(user),
			[]byte(fmt.Sprintf("key_%03d", i)),
			[]byte(fmt.Sprintf("val_FIRST_PASS_%03d", i)),
			noType,
		)
	}
	time.Sleep(1 * time.Second)
	for i := 50; i < 150; i++ {
		engine.Put([]byte(user),
			[]byte(fmt.Sprintf("key_%03d", i)),
			[]byte(fmt.Sprintf("val_SECOND_PASS_%03d", i)),
			noType,
		)
	}

	// Delete some

	engine.Delete([]byte(user), []byte("key_000"))
	engine.Delete([]byte(user), []byte("key_000"))
	engine.Delete([]byte(user), []byte("key_114"))

	// Search

	keysToSearch := []string{
		"key_000",
		"key_002",
		"key_008",
		"key_910",
		"key_045",
		"key_087",
		"key_012",
		"key_003",
		"key_013",
		"key_123",
		"key_033",
		"key_043",
		"key_053",
		"key_063",
		"key_073",
		"key_083",
		"key_193",
		"key_114",
		"key_124",
		"key_134",
		"key_134",
	}
	for _, key := range keysToSearch {
		rec, found := engine.Get([]byte(user), []byte(key))
		v := rec.Value
		if found {
			fmt.Printf("%s found: ", key)
			fmt.Println(string(v))
		} else {
			fmt.Printf("%s not found\n", key)
		}
	}
}
