package coreeng

import (
	"bufio"
	"fmt"
	"nakevaleng/core/memtable"
	"nakevaleng/core/wal"
	"nakevaleng/engine/coreconf"
	"os"
	"time"

	"nakevaleng/core/lru"
	"nakevaleng/core/record"
	"nakevaleng/core/sstable"
	"nakevaleng/ds/bloomfilter"
	"nakevaleng/ds/tokenbucket"
	"nakevaleng/util/filename"
)

type CoreEngine struct {
	conf  *coreconf.CoreConfig
	cache *lru.LRU
	mt    *memtable.Memtable
	wal   *wal.WAL
}

func New(conf *coreconf.CoreConfig) (*CoreEngine, error) {
	// Since New uses CoreConfig (which should always have valid values), we don't need to check for errors here
	lru, _ := lru.New(conf.CacheCapacity)
	memtable, _ := memtable.New(conf)
	wal, _ := wal.New(conf.WalPath, conf.DBName, conf.WalMaxRecsInSeg, conf.WalLwmIdx, conf.WalBufferCapacity)

	return &CoreEngine{
		conf,
		lru,
		memtable,
		wal,
	}, nil
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
	rec, exists := cen.mt.Find(key)
	if exists {
		cen.cache.Set(rec)
		if rec.IsDeleted() {
			return record.Record{}, false
		}
		return rec, true
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

			cen.cache.Set(rec) // Even if it's deleted, it might get searched for, so we cache it.

			if rec.IsDeleted() {
				return record.Record{}, false
			}
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
		tb, _ := tokenbucket.New(cen.conf.TokenBucketTokens, cen.conf.TokenBucketInterval)
		return *tb
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
	isTokenBucket := !cen.IsLegal(rec.Key)
	if !isTokenBucket {
		cen.wal.BufferedAppend(rec)
	}
	cen.cache.Set(rec)
	cen.mt.Add(rec)

	fmt.Printf("[DBG]\t[Memtable] Wrote %d bytes for %s\n", rec.TotalSize(), string(rec.Key))

	if cen.mt.ShouldFlush() {
		cen.mt.Flush()
		cen.FlushWALBuffer()
		cen.wal.DeleteOldSegments()
	}
}

func (cen CoreEngine) Delete(user, key []byte) bool {
	legal := cen.IsLegal(key)
	if !legal {
		// todo might want to handle this somewhere else by returning err
		//fmt.Println("ILLEGAL QUERY:", key)
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

func (cen CoreEngine) FlushWALBuffer() {
	cen.wal.FlushBuffer()
}

func main() {
	engine, _ := New(coreconf.LoadConfig("conf.yaml"))
	test(*engine)
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

	// flush WAL
	engine.FlushWALBuffer()

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
	engine.FlushWALBuffer()
}
