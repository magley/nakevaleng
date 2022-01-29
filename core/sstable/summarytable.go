package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	filename "nakevaleng/util"
	"os"
)

type summaryTableEntry struct {
	KeySize uint64 // Size of Key (in bytes)
	Offset  int64  // Offset in the index table (in bytes)
	Key     []byte // Actual key
}

// serialize appends the contents of the summaryTableEntry using a buffered writer in a binary file.
func (entry summaryTableEntry) serialize(writer *bufio.Writer) {
	err := binary.Write(writer, binary.LittleEndian, entry.KeySize)
	err = binary.Write(writer, binary.LittleEndian, entry.Offset)
	err = binary.Write(writer, binary.LittleEndian, entry.Key)

	if err != nil {
		panic(err.Error())
	}
}

// makeSummaryTable creates a new summary table from the entries stored in an index table
//	path      `path to the directory where the table will be created`
//	dbname    `name of the database`
//	level     `lsm tree level this table belongs to`
//	run       `ordinal number of the run on the given level for this table`
//	interval  `how many index entries make up a single summary entry.
//			  non-positive values are not allowed. if unsure, use 2`
func makeSummaryTable(path string, dbname string, level int, run int, interval int) {
	if interval <= 0 {
		panic("makeSummaryTable() :: interval must be >= 1!")
	}

	fnameIndex := filename.Table(path, dbname, level, run, filename.TypeIndex)
	fnameSummary := filename.Table(path, dbname, level, run, filename.TypeSummary)

	fI, err := os.Open(fnameIndex)
	if err != nil {
		panic(err)
	}
	defer fI.Close()

	fS, err := os.Create(fnameSummary)
	if err != nil {
		panic(err)
	}
	defer fI.Close()

	r := bufio.NewReader(fI)
	w := bufio.NewWriter(fS)
	defer w.Flush()

	summary := summaryTableEntry{} // the STE
	k := 0                         // counter for how many ITEs we've read for the current STE
	offset := int64(0)             // current offset in the ITE file
	isEof := false                 // is it EOF?

	for !isEof {
		index := indexTableEntry{}
		isEof = index.deserialize(r)

		// Propagate minimal value of each index entry batch for the current summary entry.
		if k == 0 {
			summary.KeySize = index.KeySize
			summary.Key = index.Key
			summary.Offset = offset
		}

		// Update counter and offset.
		k += 1
		offset += index.totalSize()

		// Write summary entry because A) we read through interval-many index entries or B) EOF
		if k%interval == 0 || isEof {
			k = 0
			summary.serialize(w)
		}
	}
}

// FindSparseIndex tries to find the byte offset of an ITE from its key inside the summary file.
//	fname   `full filename (including path) of the SUMMARY file, assumed to be valid`
//	key     `key to look for`
//
//	returns `byte offset inside the index table to look from, or -1 if not found`
func FindSparseIndex(fname string, key []byte) (offset int64) {
	f, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	r := bufio.NewReader(f)

	ste := summaryTableEntry{}
	stePrev := summaryTableEntry{Offset: -1}

	for true {
		err := binary.Read(r, binary.LittleEndian, &ste.KeySize)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return -1
		}

		err = binary.Read(r, binary.LittleEndian, &ste.Offset)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return -1
		}

		ste.Key = make([]byte, ste.KeySize)
		err = binary.Read(r, binary.LittleEndian, &ste.Key)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return -1
		}

		cmp := bytes.Compare(key, ste.Key)

		if cmp == 0 {
			return ste.Offset
		} else if cmp > 0 {
			stePrev = ste
		} else if cmp < 0 {
			return stePrev.Offset
		}
	}

	return -1
}
