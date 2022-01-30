package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

// summaryTableEntry (STE) is the building block of a Summary Table.
// Each block of ITEs is assigned a single STE, whose key matches that of the last ITE in the block.
type summaryTableEntry struct {
	KeySize uint64
	Offset  int64
	Key     []byte
}

// summaryTableHeader (STH) is a special record written at the start of a Summary Table.
// The STH only keeps the effective range of all STEs in the corresponding Summary Table.
type summaryTableHeader struct {
	MinKeySize uint64
	MaxKeySize uint64
	MinKey     []byte
	MaxKey     []byte
}

// CalcSize returns the total effective size of the STE in bytes.
func (ste summaryTableEntry) CalcSize() int64 {
	return int64(8 + 8 + ste.KeySize)
}

// CalcSize returns the total effective size of the STH in bytes.
func (sth summaryTableHeader) CalcSize() int64 {
	return int64(8 + 8 + sth.MinKeySize + sth.MaxKeySize)
}

// Write appends the contents of the STE into a binary file. The order of the attributes is:
//	KeySize, Offset, Key
func (ste summaryTableEntry) Write(writer *bufio.Writer) {
	binary.Write(writer, binary.LittleEndian, ste.KeySize)
	binary.Write(writer, binary.LittleEndian, ste.Offset)
	binary.Write(writer, binary.LittleEndian, ste.Key)
}

// Write appends the contents of the STH into a binary file. The order of the attributes is:
//	MinKeySize, MaxKeySize, MinKey, MaxKey
func (sth summaryTableHeader) Write(writer *bufio.Writer) {
	binary.Write(writer, binary.LittleEndian, sth.MinKeySize)
	binary.Write(writer, binary.LittleEndian, sth.MaxKeySize)
	binary.Write(writer, binary.LittleEndian, sth.MinKey)
	binary.Write(writer, binary.LittleEndian, sth.MaxKey)
}

// Read reads data from a binary file into an STE. Old data in the STH is overwritten. The order of
// the attributes to be read is:
//	KeySize, Offset, Key
// KeySize determines how many bytes to read for the Key field.
// Returns true if an unexpected EOF error is caught (io.EOF or io.ErrUnexpectedEOF).
func (ste *summaryTableEntry) Read(reader *bufio.Reader) (eof bool) {
	err := binary.Read(reader, binary.LittleEndian, &ste.KeySize)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}

	err = binary.Read(reader, binary.LittleEndian, &ste.Offset)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}

	ste.Key = make([]byte, ste.KeySize)
	err = binary.Read(reader, binary.LittleEndian, &ste.Key)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}

	if err != nil {
		panic(err.Error())
	}

	return false
}

// Read reads data from a binary file into an STH. Old data in the STH is overwritten. The order of
// the atrtibutes to be read is:
//	MinKeySize, MaxKeySize, MinKey, MaxKey
// MinKeySize and MaxKeySize determine how many bytes to read for the MinKey and MaxKey field.
// Returns true if an unexpected EOF error is caught (io.EOF or io.ErrUnexpectedEOF).
func (sth *summaryTableHeader) Read(reader *bufio.Reader) bool {
	err := binary.Read(reader, binary.LittleEndian, &sth.MinKeySize)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}

	err = binary.Read(reader, binary.LittleEndian, &sth.MaxKeySize)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}

	sth.MinKey = make([]byte, sth.MinKeySize)
	sth.MaxKey = make([]byte, sth.MaxKeySize)

	err = binary.Read(reader, binary.LittleEndian, &sth.MinKey)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}

	err = binary.Read(reader, binary.LittleEndian, &sth.MaxKey)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}

	if err != nil {
		panic(err.Error())
	}

	return false
}

// FindSummaryTableEntry searches a Summary Table, looking for an STE with the specified key.
// If no STE with the desired key is found, the return value's Offset field equals -1.
func FindSummaryTableEntry(summaryTableFname string, key []byte) summaryTableEntry {
	f, err := os.Open(summaryTableFname)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	r := bufio.NewReader(f)

	// Header

	sth := summaryTableHeader{}
	sth.Read(r)

	cmp := bytes.Compare(sth.MinKey, key)
	if cmp > 0 {
		return summaryTableEntry{Offset: -1}
	}
	cmp = bytes.Compare(key, sth.MaxKey)
	if cmp > 0 {
		return summaryTableEntry{Offset: -1}
	}

	// STEs

	ste := summaryTableEntry{}

	for {
		if ste.Read(r) {
			ste.Offset = -1
			break
		}

		cmp := bytes.Compare(key, ste.Key)

		if cmp <= 0 {
			break
		}
	}

	return ste
}
