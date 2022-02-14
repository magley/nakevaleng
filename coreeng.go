package main

import (
	"bufio"
	"fmt"
	"nakevaleng/core/wal"
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

const (
	path    = "data/"
	walPath = "data/log/"
	dbname  = "nakevaleng"

	SKIPLIST_LEVEL       = 3
	SKIPLIST_LEVEL_MAX   = 5
	MEMTABLE_CAPACITY    = 10
	CACHE_CAPACITY       = 5
	LSM_LVL_MAX          = 4
	LSM_RUN_MAX          = 4
	TOKENBUCKET_TOKENS   = 50
	TOKENBUCKET_INTERVAL = 1
	WAL_MAX_RECS_IN_SEG  = 5
	WAL_LWM_IDX          = 2
	WAL_BUFFER_CAPACITY  = 5

	INTERNAL_START = "$"
)

type CoreEngine struct {
	cache lru.LRU
	sl    skiplist.Skiplist
	wal   wal.WAL
	// others are in const for now
}

func New() *CoreEngine {
	return &CoreEngine{
		*lru.New(CACHE_CAPACITY),
		skiplist.New(SKIPLIST_LEVEL, SKIPLIST_LEVEL_MAX),
		*wal.New(walPath, dbname, WAL_MAX_RECS_IN_SEG, WAL_LWM_IDX, WAL_BUFFER_CAPACITY),
	}
}

// CheckLegality returns true if legal key, otherwise false
func (cen *CoreEngine) CheckLegality(key []byte) bool {
	start := []byte(INTERNAL_START)
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

func (cen *CoreEngine) Get(user, key []byte) []byte {
	legal := cen.CheckLegality(key)
	if !legal {
		// todo might want to handle this somewhere else
		fmt.Println("ILLEGAL QUERY:", key)
		return nil
	}
	tb := cen.getTokenBucket(user)
	for true {
		if tb.HasEnoughTokens() {
			break
		} else {
			fmt.Println("Slow down!")
			time.Sleep(1 * time.Second)
		}
	}
	cen.putTokenBucket(user, tb)
	return cen.get(key)
}

// Get without checking legality or getting token buckets
func (cen *CoreEngine) get(key []byte) []byte {
	// todo check wal timestamps
	// Memtable, sort of

	n := cen.sl.Find(key, true)
	if n != nil {
		nRec := n.Data
		cen.cache.Set(nRec)
		//fmt.Println("[found in skiplist]")
		return nRec.Value
	}

	// Cache

	r, foundInCache := cen.cache.Get(string(key))
	if foundInCache {
		cen.cache.Set(r)
		//fmt.Println("[cache hit]")
		return r.Value
	}

	// Disk

	greatestLevel := filename.GetLastLevel(path, dbname)

	for j := 1; j <= greatestLevel; j++ {
		greatestRun := filename.GetLastRun(path, dbname, j)

		for i := greatestRun; i >= 0; i-- {
			// Filter

			q := bloomfilter.
				DecodeFromFile(filename.Table(path, dbname, j, i, filename.TypeFilter)).
				Query(key)

			if !q {
				//fmt.Printf("%s Not found @ [FILTER] @ L%d R%d\n", key, j, i)
				continue
			}

			// Summary

			ste := sstable.FindSummaryTableEntry(
				filename.Table(path, dbname, j, i, filename.TypeSummary),
				key,
			)

			//fmt.Println("AA", ste.Offset, key, j, i)

			if ste.Offset == -1 {
				//fmt.Printf("%s Not found @ [SUMMARY] @ L%d R%d\n", key, j, i)
				continue
			}

			// Index

			ite := sstable.FindIndexTableEntry(
				filename.Table(path, dbname, j, i, filename.TypeIndex),
				key,
				ste.Offset,
			)

			//fmt.Println("BB", ite.Offset, key)

			if ite.Offset == -1 {
				//fmt.Printf("%s Not found @ [INDEX] @ L%d R%d\n", key, j, i)
				continue
			}

			// Data

			f, _ := os.Open(filename.Table(path, dbname, j, i, filename.TypeData))
			defer f.Close()
			r := bufio.NewReader(f)

			f.Seek(ite.Offset, 0)
			rec := record.Record{}
			rec.Deserialize(r)

			cen.cache.Set(rec)
			return rec.Value
		}
	}

	return nil
}

func (cen *CoreEngine) getTokenBucket(user []byte) tokenbucket.TokenBucket {
	tbKey := []byte(INTERNAL_START)
	tbKey = append(tbKey, user...)
	tbBytes := cen.get(tbKey)
	if tbBytes == nil {
		return *tokenbucket.New(TOKENBUCKET_TOKENS, TOKENBUCKET_INTERVAL)
	}
	return tokenbucket.FromBytes(cen.get(tbKey))
}

func (cen *CoreEngine) putTokenBucket(user []byte, bucket tokenbucket.TokenBucket) {
	tbKey := []byte(INTERNAL_START)
	tbKey = append(tbKey, user...)
	cen.put(tbKey, bucket.ToBytes())
}

func (cen *CoreEngine) Put(user, key, val []byte) bool {
	legal := cen.CheckLegality(key)
	if !legal {
		// todo might want to handle this somewhere else by returning err
		fmt.Println("ILLEGAL QUERY:", key)
		return false
	}
	tb := cen.getTokenBucket(user)
	for true {
		if tb.HasEnoughTokens() {
			break
		} else {
			fmt.Println("Slow down!")
			time.Sleep(1 * time.Second)
		}
	}
	cen.putTokenBucket(user, tb)
	cen.put(key, val)
	return true
}

func (cen *CoreEngine) put(key, val []byte) {
	rec := record.New(key, val)
	// assume only TokenBuckets can be illegal for now, todo might want to change to TypeInfo
	isTokenBucket := !cen.CheckLegality(key)
	if !isTokenBucket {
		cen.wal.BufferedAppend(rec)
	}
	cen.cache.Set(rec)
	cen.sl.Write(rec)

	if cen.sl.Count > MEMTABLE_CAPACITY {
		newRun := filename.GetLastRun(path, dbname, 1) + 1
		sstable.MakeTable(path, dbname, 1, newRun, &cen.sl)
		cen.sl.Clear()
		lsmtree.Compact(path, dbname, 1, LSM_LVL_MAX, LSM_RUN_MAX)
	}
}

func (cen *CoreEngine) Delete(user, key []byte) bool {
	//todo
	return false
}

func main() {
	engine := New()
	test(engine)
}

func test(engine *CoreEngine) {
	// Search
	sleepForOneSecondAfterHowManyRecords := 20000

	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			engine.Put([]byte("USER"),
				[]byte(fmt.Sprintf("key_%03d", i)),
				[]byte(fmt.Sprintf("val_e_%03d", i)),
			)
		} else {
			engine.Put([]byte("USER"),
				[]byte(fmt.Sprintf("key_%03d", i%20)),
				[]byte(fmt.Sprintf("val_o_%03d", i/20)),
			)
		}

		if i != 0 && i%sleepForOneSecondAfterHowManyRecords == 0 {
			time.Sleep(1 * time.Second)
		}
	}

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
		"key_023",
		"key_033",
		"key_043",
		"key_053",
		"key_063",
		"key_073",
		"key_083",
		"key_093",
		"key_014",
		"key_024",
		"key_034",
		"key_834",
	}
	for _, key := range keysToSearch {
		v := engine.Get([]byte("USER"), []byte(key))
		if v != nil {
			fmt.Printf("%s found: ", key)
			fmt.Println(string(v))
		} else {
			fmt.Printf("%s not found\n", key)
		}
	}
}
