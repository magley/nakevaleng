```
memtable
    - uses a skiplist to store records
    - when full, forms an SStable that gets flushed to disk
```

```go

conf := coreconf.LoadConfig("config.yaml")
memtable := memtable.New(conf)

rec1 := record.NewFromString("key01", "val01")
rec2 := record.NewFromString("key02", "val02")
rec3 := record.NewFromString("key03", "val03")
rec4 := record.NewFromString("key04", "val04")
rec5 := record.NewFromString("key05", "val05")

memtable.Add(rec1)
memtable.Add(rec2)
memtable.Add(rec3)
memtable.Add(rec4)

memtable.Remove("key01") // rec1 has its tombstone bit set

nrec, isPresent := memtable.Find("key02") // will return the record and true
nrec, isPresent = memtable.Find("key12") // will return an empty record and false

isFull := memtable.Add(rec5) // will return true
if isFull {
    memtable.Flush()

    // Assuming a WAL is present as well
    wal.DeleteOldSegments()
}

```