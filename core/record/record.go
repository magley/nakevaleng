// Package record implements a Record structure containing all necessary
// information for usage in the system, as well as various helper functions.
package record

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

const (
	RECORD_STATUS_DEFAULT    = 0 << 0
	RECORD_TOMBSTONE_REMOVED = 1 << 0
)

// Iterator iterates over records
// the bool is true if the returned one is one after the last element
// (passed all records once) this iterates circularly, so it is expected to pass through all at once
type Iterator func() (Record, bool)

// Atomic unit of information with all required context.
type Record struct {
	Crc       uint32 // Checksum of key and value ONLY!!!
	Timestamp int64  // Creation time as UNIX timestamp
	Status    uint8  // Status bits, see the documentation for more info.
	TypeInfo  uint8  // Type ID of 'Value'. Defaults to 0 (no type).
	KeySize   uint64 // Size of Key (in bytes)
	ValueSize uint64 // Size of Value (in bytes)
	Key       []byte //
	Value     []byte //
}

// Minimal context of a Record required to perform major compaction.
type KeyContext struct {
	Key     []byte
	RecSize uint64 // Size of the Record object this context was built from, using .TotalSize().
}

// TotalSize calculates the total number of bytes required to store the given record structure.
func (rec Record) TotalSize() uint64 {
	return 4 + 8 + 1 + 1 + 8 + 8 + rec.KeySize + rec.ValueSize
}

// New creates a Record object with the key and value specified as byte slices.
func New(key, val []byte) Record {
	return Record{
		Crc:       crc32.ChecksumIEEE(append(key[:], val[:]...)),
		Timestamp: time.Now().Unix(),
		Status:    RECORD_STATUS_DEFAULT,
		TypeInfo:  0,
		KeySize:   uint64(len(key)),
		ValueSize: uint64(len(val)),
		Key:       key,
		Value:     val,
	}
}

// NewTyped creates a Record object with the given key and value and assigns it a type.
// Note that nakevaleng does not provide any context for the types.
func NewTyped(key, val []byte, typeinfo uint8) Record {
	r := New(key, val)
	r.TypeInfo = typeinfo
	return r
}

// NewFromString is like New but with key and val specified as strings.
func NewFromString(key, val string) Record {
	return New([]byte(key), []byte(val))
}

// Clone creates a new Record object with fields the copied from 'rec'.
// The timestamp is NOT copied!
func Clone(rec Record) Record {
	return Record{
		Crc:       rec.Crc,
		Timestamp: time.Now().Unix(),
		Status:    rec.Status,
		TypeInfo:  rec.TypeInfo,
		KeySize:   rec.KeySize,
		ValueSize: rec.ValueSize,
		Key:       rec.Key,
		Value:     rec.Value,
	}
}

// NewEmpty creates an empty Record object.
func NewEmpty() Record {
	return New(make([]byte, 0), make([]byte, 0))
}

// IsDeleted checks for the Tombstone bit in the record's Status field.
func (rec Record) IsDeleted() bool {
	return (rec.Status & RECORD_TOMBSTONE_REMOVED) != 0
}

// String returns a string representation of the record suitable for reading and debugging.
// The Status and TypeInfo fields are printed in binary.
func (rec Record) String() string {
	return fmt.Sprintf("Record(%d %d %08b %08b %d %d %v %v)",
		rec.Crc,
		rec.Timestamp,
		rec.Status,
		rec.TypeInfo,
		rec.KeySize,
		rec.ValueSize,
		string(rec.Key),
		string(rec.Value),
	)
}

// Deserialize reads data from buffered reader and overwrites this record.
// The checksum is recalculated and compared with the one read from the file.
// The function will panic if they don't match.
// If the reader reaches an EOF, eof will be set to true.
func (rec *Record) Deserialize(reader *bufio.Reader) (eof bool) {
	eof = false

	err := binary.Read(reader, binary.LittleEndian, &rec.Crc)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}
	err = binary.Read(reader, binary.LittleEndian, &rec.Timestamp)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}
	err = binary.Read(reader, binary.LittleEndian, &rec.Status)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}
	err = binary.Read(reader, binary.LittleEndian, &rec.TypeInfo)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}
	err = binary.Read(reader, binary.LittleEndian, &rec.KeySize)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}
	err = binary.Read(reader, binary.LittleEndian, &rec.ValueSize)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}

	rec.Key = make([]byte, rec.KeySize)
	rec.Value = make([]byte, rec.ValueSize)

	err = binary.Read(reader, binary.LittleEndian, &rec.Key)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}
	err = binary.Read(reader, binary.LittleEndian, &rec.Value)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return true
	}

	if err != nil {
		panic(err.Error())
	}

	// Checksum
	crc := crc32.ChecksumIEEE(append(rec.Key[:], rec.Value[:]...))

	if crc != rec.Crc {
		errMsg := fmt.Sprint("Bad Record checksum (got ", crc, ", expected ", rec.Crc, ")\n", rec.String())
		panic(errMsg)
	}

	return false
}

// ToBytes creates a binary slice of all data for the Record object.
func (rec Record) ToBytes() []byte {
	buffer := make([]byte, 0, rec.TotalSize())
	w := bytes.NewBuffer(buffer)
	binary.Write(w, binary.LittleEndian, rec.Crc)
	binary.Write(w, binary.LittleEndian, rec.Timestamp)
	binary.Write(w, binary.LittleEndian, rec.Status)
	binary.Write(w, binary.LittleEndian, rec.TypeInfo)
	binary.Write(w, binary.LittleEndian, rec.KeySize)
	binary.Write(w, binary.LittleEndian, rec.ValueSize)
	binary.Write(w, binary.LittleEndian, rec.Key)
	binary.Write(w, binary.LittleEndian, rec.Value)
	return w.Bytes()
}

// Serialize appends the contents of the Record using a buffered writer, in binary mode.
// The writer does not get flushed. It's up to the caller to invoke writer.Flush().
func (rec Record) Serialize(writer *bufio.Writer) {
	err := binary.Write(writer, binary.LittleEndian, rec.Crc)
	err = binary.Write(writer, binary.LittleEndian, rec.Timestamp)
	err = binary.Write(writer, binary.LittleEndian, rec.Status)
	err = binary.Write(writer, binary.LittleEndian, rec.TypeInfo)
	err = binary.Write(writer, binary.LittleEndian, rec.KeySize)
	err = binary.Write(writer, binary.LittleEndian, rec.ValueSize)
	err = binary.Write(writer, binary.LittleEndian, rec.Key)
	err = binary.Write(writer, binary.LittleEndian, rec.Value)

	if err != nil {
		panic(err.Error())
	}
}
