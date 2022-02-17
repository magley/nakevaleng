package sstable

import (
	"bufio"
	"nakevaleng/core/record"
	"nakevaleng/util/filename"
	"os"
)

// makeDataTable writes the contents from the skiplist into a data table file, record by record.
//	path    `path to the directory where the table will be created`
//	dbname  `name of the database`
//	level   `lsm tree level this table belongs to`
//	run     `ordinal number of the run on the given level for this table`
//	list    `skiplist containing all data to read from when writing to the file`
func makeDataTable(path string, dbname string, level int, run int, rit record.Iterator) {
	fname := filename.Table(path, dbname, level, run, filename.TypeData)
	f, err := os.Create(fname)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()

	for rec, last := rit(); !last; rec, last = rit() {
		rec.Serialize(w)
	}
}
