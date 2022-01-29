package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"nakevaleng/core/skiplist"
	filename "nakevaleng/util"
	"os"
)

type indexTableEntry struct {
	KeySize uint64 // Size of Key (in bytes)
	Offset  int64  // Offset in the data table (in bytes)
	Key     []byte // Actual key
}

// totalSize returns the total amount of bytes required to store the given indexTableEntry
func (entry indexTableEntry) totalSize() int64 {
	return int64(8 + 8 + entry.KeySize)
}

// serialize appends the contents of the indexTableEntry using a buffered writer in a binary file.
func (entry indexTableEntry) serialize(writer *bufio.Writer) {
	err := binary.Write(writer, binary.LittleEndian, entry.KeySize)
	err = binary.Write(writer, binary.LittleEndian, entry.Offset)
	err = binary.Write(writer, binary.LittleEndian, entry.Key)

	if err != nil {
		panic(err.Error())
	}
}

// deserialize reads the contents from a file in binary mode at current position into the entry.
// returns true if end of file, else false
func (entry *indexTableEntry) deserialize(reader *bufio.Reader) (eof bool) {
	err := binary.Read(reader, binary.LittleEndian, &entry.KeySize)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}
	err = binary.Read(reader, binary.LittleEndian, &entry.Offset)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}
	entry.Key = make([]byte, entry.KeySize)
	err = binary.Read(reader, binary.LittleEndian, &entry.Key)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}

	if err != nil {
		panic(err.Error())
	}

	return false
}

// makeIndexTable creates a new index table from data given as a skiplist.
//	path    `path to the directory where the table will be created`
//	dbname  `name of the database`
//	level   `lsm tree level this table belongs to`
//	run     `ordinal number of the run on the given level for this table`
//	list    `skiplist containing all data to read from when writing to the file`
func makeIndexTable(path string, dbname string, level int, run int, list *skiplist.Skiplist) {
	fname := filename.Table(path, dbname, level, run, filename.TypeIndex)
	f, err := os.Create(fname)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()

	offset := int64(0)
	n := list.Header.Next[0]

	for n != nil {
		entry := indexTableEntry{
			KeySize: n.Data.KeySize,
			Offset:  offset,
			Key:     n.Data.Key,
		}
		offset += int64(n.Data.TotalSize())

		entry.serialize(w)
		n = n.Next[0]
	}
}

// FindIndex tries to find the byte offset of a record from its key inside the index file.
//	fname  `full filename (including path) of the INDEX file, assumed to be valid`
//	key    `key to look for`
//	offset `start offset for searching (you get this from the summary table, otherwise set it to 0)`
//
//	returns `byte offset inside the data table or -1 if not found`
func FindIndex(fname string, key []byte, startoffset int64) (offset int64) {
	f, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	f.Seek(startoffset, 0)

	for {
		e := indexTableEntry{}

		err := binary.Read(r, binary.LittleEndian, &e.KeySize)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return -1
		}

		err = binary.Read(r, binary.LittleEndian, &e.Offset)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return -1
		}

		e.Key = make([]byte, e.KeySize)
		err = binary.Read(r, binary.LittleEndian, &e.Key)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return -1
		}

		cmp := bytes.Compare(key, e.Key)

		if cmp == 0 {
			return e.Offset
		} else if cmp < 0 {
			// Index file is sorted, so we can terminate early.
			return -1
		}
	}
}
