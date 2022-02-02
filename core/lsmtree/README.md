```
lsmtree

LSM Tree with size-tiered compaction:
    - when the number of runs on level 1 reaches a treshold, compaction happens
    - during the compaction process, all SSTables on level 1 are merged into 1 new table
    - compaction will not happen if the level isn't full
    - the new table is written to level 2
    - in case the number of runs on level 2 reaches a treshold, compaction happens on level 2
    - this is called chaining
    - there is a maximum number of levels - K - which the database can have
    - compaction cannot happen on level K
    - the Data table is kept in memory while all other tables are built in-place
    - this can result in large memory usage when doing compaction, but building the merkle tree requires all nodes to be known
    
Merging process:
    - nakevaleng uses a basic k-way merge algorithm
    - the priority queue is implemented with a slice that gets sorted on each iteration
    - there's room for a performance gain here, by using heaps
    - a conflict happens where there the same key is present in multiple SSTables
    - in that case, only the record with the greatest timestamp (i.e. most recent record) is used
    - the others are discarded
```

```go

Compact("data/", "nakevaleng", 1)

```