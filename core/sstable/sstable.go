package sstable

import (
	"nakevaleng/core/skiplist"
)

func MakeTable(path string, dbname string, level int, run int, list *skiplist.Skiplist) {
	makeDataTable(path, dbname, level, run, list)
	makeIndexTable(path, dbname, level, run, list)
	makeSummaryTable(path, dbname, level, run, 2)
}
