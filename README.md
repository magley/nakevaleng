# nakevaleng
Nakevaleng is a key-value database
## Getting started
To start the engine just run it with a conf.yaml file in the same directory
### conf.yaml
Let's take a look at the default configuration:
```yaml
path: data/
wal_path: data/log/
db_name: nakevaleng
skiplist_level: 3
skiplist_level_max: 5
memtable_capacity: 10
memtable_threshold: 2KB
memtable_flush_strategy: 3
cache_capacity: 5
summary_page_size: 3
lsm_lvl_max: 4
lsm_run_max: 4
token_bucket_tokens: 100
token_bucket_interval: 1
wal_max_recs_in_seg: 5
wal_lwm_idx: 0
wal_buffer_capacity: 5
internal_start: $
```
- **path** represents the path to where the database will be kept
- **wal_path** represents the path to where the log files will be written
- **db_name** is the name of the database you are making: it affects the filenames the engine uses
- **skiplist_level** is the starting level of the Skip list used for storing data in memory
- **skiplist_level_max** is the maximum level of the Skip list
- **memtable_capacity** is the number of elements allowed to exist in the Memtable at once before flushing
- **memtable_threshold** is the size the Memtable has to reach to be flushed on disk
- **memtable_flush_strategy** is the strategy to use for flushing. 1 is by capacity, 2 is by threshold, 3 is to apply both (whichever gets it's condition first)
- **cache_capacity** is the max amount of Records to be cached at any one time
- **summary_page_size** is the amount of keys to skip in writing when making an SSTable summary. Bigger page size = smaller summary
- **lsm_lvl_max** is the max level to which compaction goes
- **lsm_run_max** is the number of runs in a single level, except the last level which is infinite
- **token_bucket_tokens** is the number of requests a user can make in a given time frame
- **token_bucket_interval** is the interval after which the users requests cap resets. Measured in seconds
- **wal_max_recs_in_seg** is the amount of records written to a log file before switching to a new log file
- **wal_lwm_idx** is the number of log files kept after flushing the Memtable to disk or just deleting old segments
- **wal_buffer_capacity** is the amount of records to keep in the log buffer before flushing to disk
- **internal_start** is a string that denotes the start of keys that are for the engine's internal use only. Used for token buckets
