```
wal
    - write ahead log
    - supports batch mode
    - uses mmap for reading/writing
    - segmented
    - segments divided by number of records
```

```go

walPath := "data/log/"
dbname := "nakevaleng"
maxRecsInSeg := 3
lwmIndex := 1
buffCap := 2

// Will create a segment if there are none in walPath
wal := wal.New(walPath, dbname, maxRecsInSeg, lwmIndex, buffCap)

rec1 := record.NewFromString("Key01", "Val01")
rec2 := record.NewFromString("Key02", "Val02")
rec3 := record.NewFromString("Key03", "Val03")
rec4 := record.NewFromString("Key04", "Val04")

// Immediately appends to the last segment
wal.Append(rec1)

// Writes to a buffer in memory
// Flushes manually, or automatically when the buffer is filled up (buffCap)
wal.BufferedAppend(rec2)
wal.BufferedAppend(rec3) // Automatic flushing happens here (buffCap is reached)

wal.BufferedAppend(rec4) // Has to be flushed manually

// Manual flush
wal.FlushBuffer()

// Will read only rec4
// rec1, rec2 and rec3 are in the previous segment (maxRecsInSeg is 3)
recs := wal.ReadLastSegment()

// Will read rec1, rec2 and rec3
recs = wal.ReadSegmentAt(0)

// Will panic, as 2 is out of bounds
wal.ReadSegmentAt(2)

// Will read rec1, rec2, rec3 and rec4
recs = wal.ReadAllSegments()

// Let's add more records
rec5 := record.NewFromString("Key05", "Val05")
rec6 := record.NewFromString("Key06", "Val06")
rec7 := record.NewFromString("Key07", "Val07")
rec8 := record.NewFromString("Key08", "Val08")
wal.BufferedAppend(rec5)
wal.BufferedAppend(rec6)
wal.BufferedAppend(rec7)
wal.BufferedAppend(rec8)
wal.Flush() // We should always manually flush, even though it isn't necessary here

// Now we have 3 segments:
// seg0: rec1, rec2, rec3
// seg1: rec4, rec5, rec6
// seg2: rec7, rec8

// Will read rec4, rec5, rec6, rec7, rec8
recs = wal.ReadSegmentsInRange(1, 3) // note: the range is [1, 3)

// Deletes old segments based on the low water mark index
wal.DeleteOldSegments()

// Now the segments look like this:
// seg0: rec4, rec5, rec6
// seg1: rec7, rec8

wal.ResetLastSegment() // seg1 now has no records

```