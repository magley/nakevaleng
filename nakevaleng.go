package main

import (
	"bufio"
	"fmt"
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

//const (
//	path    = "data/"
//	walPath = "data/log/"
//	dbname  = "nakevaleng"
//
//	SKIPLIST_LEVEL       = 3
//	SKIPLIST_LEVEL_MAX   = 5
//	MEMTABLE_CAPACITY    = 10
//	CACHE_CAPACITY       = 5
//	LSM_LVL_MAX          = 4
//	LSM_RUN_MAX          = 4
//	TOKENBUCKET_TOKENS   = 50
//	TOKENBUCKET_INTERVAL = 1
//	WAL_MAX_RECS_IN_SEG  = 5
//	WAL_LWM_IDX          = 2
//	WAL_BUFFER_CAPACITY  = 5
//)

func main() {
	cache := lru.New(CACHE_CAPACITY)
	skipli := skiplist.New(SKIPLIST_LEVEL, SKIPLIST_LEVEL_MAX)
	tb := tokenbucket.New(TOKENBUCKET_TOKENS, TOKENBUCKET_INTERVAL)

	insert02(cache, &skipli, tb)

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
		r, found := search(key, cache, &skipli, tb)
		if found {
			fmt.Printf("%s\n", r.String())
		} else {
			fmt.Printf("%s not found\n", key)
		}
	}
}

// insert01: All keys are different.
func insert01(cache *lru.LRU, skipli *skiplist.Skiplist, tb *tokenbucket.TokenBucket) {
	dataToInsert := []record.Record{}
	for i := 0; i < 100; i++ {
		dataToInsert = append(dataToInsert, record.NewFromString(
			fmt.Sprintf("key_%03d", i),
			fmt.Sprintf("val_%03d", i),
		))
	}
	insert(dataToInsert, cache, skipli, tb)
}

// insert02: Odd keys are added only once, even keys are added multiple times.
func insert02(cache *lru.LRU, skipli *skiplist.Skiplist, tb *tokenbucket.TokenBucket) {
	sleepForOneSecondAfterHowManyRecords := 20
	dataToInsert := []record.Record{}

	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			dataToInsert = append(dataToInsert, record.NewFromString(
				fmt.Sprintf("key_%03d", i),
				fmt.Sprintf("val_e_%03d", i),
			))
		} else {
			dataToInsert = append(dataToInsert, record.NewFromString(
				fmt.Sprintf("key_%03d", i%20),
				fmt.Sprintf("val_o_%03d", int(i/20)),
			))
		}

		if i != 0 && i%sleepForOneSecondAfterHowManyRecords == 0 {
			time.Sleep(1 * time.Second)
		}
	}

	insert(dataToInsert, cache, skipli, tb)
}

// Don't call this from main(), use insertXX().
func insert(dataToInsert []record.Record, cache *lru.LRU, skipli *skiplist.Skiplist, tb *tokenbucket.TokenBucket) {
	for _, rec := range dataToInsert {
		for true {
			if tb.HasEnoughTokens() {
				break
			} else {
				fmt.Println("Slow down!")
				time.Sleep(1 * time.Second)
			}
		}
		cache.Set(rec)
		skipli.Write(rec)

		if skipli.Count > MEMTABLE_CAPACITY {
			newRun := filename.GetLastRun(path, dbname, 1) + 1
			sstable.MakeTable(path, dbname, 1, newRun, skipli)
			skipli.Clear()
			lsmtree.Compact(path, dbname, 1, LSM_LVL_MAX, LSM_RUN_MAX)
		}
	}
}

// This searches for one single key.
func search(key string, cache *lru.LRU, skipli *skiplist.Skiplist, tb *tokenbucket.TokenBucket) (record.Record, bool) {
	for true {
		if tb.HasEnoughTokens() {
			break
		} else {
			fmt.Println("Slow down!")
			time.Sleep(1 * time.Second)
		}
	}

	// Memtable, sort of

	n := skipli.Find([]byte(key), true)
	if n != nil {
		nRec := n.Data
		cache.Set(nRec)
		fmt.Println("[found in skiplist]")
		return nRec, true
	}

	// Cache

	r, foundInCache := cache.Get(key)
	if foundInCache {
		cache.Set(r)
		fmt.Println("[cache hit]")
		return r, true
	}

	// Disk

	greatestLevel := filename.GetLastLevel(path, dbname)

	for j := 1; j <= greatestLevel; j++ {
		greatestRun := filename.GetLastRun(path, dbname, j)

		for i := greatestRun; i >= 0; i-- {
			// Filter

			q := bloomfilter.
				DecodeFromFile(filename.Table(path, dbname, j, i, filename.TypeFilter)).
				Query([]byte(key))

			if !q {
				//fmt.Printf("%s Not found @ [FILTER] @ L%d R%d\n", key, j, i)
				continue
			}

			// Summary

			ste := sstable.FindSummaryTableEntry(
				filename.Table(path, dbname, j, i, filename.TypeSummary),
				[]byte(key),
			)

			if ste.Offset == -1 {
				//fmt.Printf("%s Not found @ [SUMMARY] @ L%d R%d\n", key, j, i)
				continue
			}

			// Index

			ite := sstable.FindIndexTableEntry(
				filename.Table(path, dbname, j, i, filename.TypeIndex),
				[]byte(key),
				ste.Offset,
			)

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

			cache.Set(rec)
			return rec, true
		}
	}

	return record.NewEmpty(), false
}
