package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// indexTableEntry (ITE) is the building block of an Index Table.
type indexTableEntry struct {
	KeySize uint64 // How many bytes does Key take
	Offset  int64  // Relative address of the record in the Data table
	Key     []byte // The key of the record
}

// CalcSize returns the total effective size of the ITE in bytes.
func (ite indexTableEntry) CalcSize() int64 {
	return int64(8 + 8 + ite.KeySize)
}

// Write appends the contents of the ITE into a binary file. The order of the attributes is:
//	KeySize, Offset, Key
func (ite indexTableEntry) Write(writer *bufio.Writer) {
	binary.Write(writer, binary.LittleEndian, ite.KeySize)
	binary.Write(writer, binary.LittleEndian, ite.Offset)
	binary.Write(writer, binary.LittleEndian, ite.Key)
}

// Read reads data from a binary file into an ITE. Old data in the ITE is overwritten. The order of
// the attributes to be read is:
//	KeySize, Offset, Key
// KeySize determines how many bytes to read for the Key field.
// Returns true if an unexpected EOF error is caught (io.EOF or io.ErrUnexpectedEOF).
func (ite *indexTableEntry) Read(reader *bufio.Reader) (eof bool) {
	err := binary.Read(reader, binary.LittleEndian, &ite.KeySize)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}

	err = binary.Read(reader, binary.LittleEndian, &ite.Offset)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}

	// todo remove two below
	fmt.Println(string(ite.Key))
	fmt.Println(ite)
	ite.Key = make([]byte, ite.KeySize)
	err = binary.Read(reader, binary.LittleEndian, &ite.Key)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}

	if err != nil {
		panic(err.Error())
	}

	return false
}

// FindIndexTableEntry searches an Index Table, looking for an ITE with the specified key. The user
// may specify an offset in bytes from which to begin the search. This value can be retrieved from
// a Summary Table Entry. For faster lookups.
// If no ITE with the desired key is found, the return value's Offset field equals -1.
func FindIndexTableEntry(indexTableFname string, key []byte, startoffset int64) indexTableEntry {
	f, err := os.Open(indexTableFname)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	r := bufio.NewReader(f)

	f.Seek(startoffset, 0)
	ite := indexTableEntry{}
	// todo remove below
	fmt.Println(indexTableFname, " ", string(key), " ", startoffset)

	for {
		if ite.Read(r) {
			ite.Offset = -1
			break
		}

		cmp := bytes.Compare(key, ite.Key)

		if cmp == 0 {
			break
		} else if cmp < 0 {
			ite.Offset = -1
			break
		}
	}

	return ite
}
