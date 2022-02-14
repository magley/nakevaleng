package wal

import (
	"bufio"
	"bytes"
	"fmt"
	"nakevaleng/core/record"
	"nakevaleng/util/filename"
	"os"

	"github.com/edsrzf/mmap-go"
)

type WAL struct {
	walPath string
	dbname  string

	segmentPaths            []string
	lastSegmentPath         string
	lastSegmentNumOfRecords int
	maxRecordsInSegment     int
	lowWaterMarkIndex       int

	appendingBufferCapacity int
	appendingBuffer         []record.Record
}

// Returns a pointer to a WAL object.
// If no segments are present in the directory, it will create one.
func New(walPath, dbname string, maxRecordsInSegment, lowWaterMarkIndex, appendingBufferCapacity int) *WAL {
	segmentPaths := filename.GetSegmentPaths(walPath, dbname)
	if len(segmentPaths) == 0 {
		lastSegmentPath := filename.Log(walPath, dbname, 0)

		file, err := os.Create(lastSegmentPath)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		segmentPaths = append(segmentPaths, lastSegmentPath)
	}

	lastSegmentPath := segmentPaths[len(segmentPaths)-1]
	lastSegmentNumOfRecords := calculateNumOfRecordsInSegment(lastSegmentPath)

	appendingBuffer := make([]record.Record, 0, appendingBufferCapacity)

	return &WAL{
		walPath:                 walPath,
		dbname:                  dbname,
		segmentPaths:            segmentPaths,
		lastSegmentPath:         lastSegmentPath,
		lastSegmentNumOfRecords: lastSegmentNumOfRecords,
		maxRecordsInSegment:     maxRecordsInSegment,
		lowWaterMarkIndex:       lowWaterMarkIndex,
		appendingBufferCapacity: appendingBufferCapacity,
		appendingBuffer:         appendingBuffer}
}

// Helper function that returns the current number of records present in the segment.
func calculateNumOfRecordsInSegment(filename string) int {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	numOfRecords := 0
	reader := bufio.NewReader(file)
	rec := record.Record{}
	for eof := rec.Deserialize(reader); !eof; eof = rec.Deserialize(reader) {
		numOfRecords++
	}

	return numOfRecords
}

// Appends a single record into the last segment. If the last segment is full,
// Append will add a new segment and append the record into the new segment.
func (wal *WAL) Append(rec record.Record) {
	if wal.lastSegmentNumOfRecords == wal.maxRecordsInSegment {
		wal.addSegment() // Append is now operating on the new last segment
	}

	file, err := os.OpenFile(wal.lastSegmentPath, os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		panic(err)
	}

	oldSize := stat.Size()
	newSize := oldSize + int64(rec.TotalSize())
	err = file.Truncate(newSize)
	if err != nil {
		panic(err)
	}

	mmapf, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		panic(err)
	}
	defer mmapf.Unmap()

	copy(mmapf[oldSize:], rec.ToBytes())

	wal.lastSegmentNumOfRecords++
}

// Appends a record into the buffer stored within the WAL.
// When the buffer is full, it will be flushed.
func (wal *WAL) BufferedAppend(rec record.Record) {
	wal.appendingBuffer = append(wal.appendingBuffer, rec)
	if len(wal.appendingBuffer) == wal.appendingBufferCapacity {
		wal.FlushBuffer()
	}
}

// Appends the buffer's records into the WAL's segments.
// Depending on the size of the buffer and the fullness of the last segment,
// FlushBuffer can cause the creation of new segments.
func (wal *WAL) FlushBuffer() {
	for len(wal.appendingBuffer) != 0 {
		if wal.lastSegmentNumOfRecords == wal.maxRecordsInSegment {
			wal.addSegment() // Append is now operating on the new last segment
		}

		numOfRecsToAppend := wal.maxRecordsInSegment - wal.lastSegmentNumOfRecords
		if numOfRecsToAppend > len(wal.appendingBuffer) {
			numOfRecsToAppend = len(wal.appendingBuffer)
		}

		partialBuffer := wal.appendingBuffer[:numOfRecsToAppend]
		wal.flushPartialBufferToSegment(partialBuffer)

		wal.appendingBuffer = wal.appendingBuffer[numOfRecsToAppend:]
		wal.lastSegmentNumOfRecords += numOfRecsToAppend
	}
}

// Utility function for adding a new segment to the WAL,
// setting it as the last segment and setting its number of records to zero.
func (wal *WAL) addSegment() {
	_, logNo, _, _ := filename.Query(wal.lastSegmentPath)
	logNo++

	newLastSegmentPath := filename.Log(wal.walPath, wal.dbname, logNo)

	file, err := os.Create(newLastSegmentPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	wal.segmentPaths = append(wal.segmentPaths, newLastSegmentPath)
	wal.lastSegmentPath = newLastSegmentPath
	wal.lastSegmentNumOfRecords = 0
}

// Helper function used by FlushBuffer to partially flush its contents to the
// segment.
func (wal *WAL) flushPartialBufferToSegment(partialBuffer []record.Record) {
	file, err := os.OpenFile(wal.lastSegmentPath, os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	sizeOfBuffer := int64(0) // number of bytes in the buffer
	bytesOfBuffer := make([]byte, 0)
	for _, rec := range partialBuffer {
		sizeOfBuffer += int64(rec.TotalSize())
		bytesOfBuffer = append(bytesOfBuffer, rec.ToBytes()...)
	}

	stat, err := file.Stat()
	if err != nil {
		panic(err)
	}

	oldSize := stat.Size()
	newSize := oldSize + sizeOfBuffer
	err = file.Truncate(newSize)
	if err != nil {
		panic(err)
	}

	mmapf, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		panic(err)
	}
	defer mmapf.Unmap()

	copy(mmapf[oldSize:], bytesOfBuffer)
}

// Returns a slice of all the records found in the last segment.
func (wal *WAL) ReadLastSegment() []record.Record {
	return readEntireSegment(wal.lastSegmentPath)
}

// Returns a slice of all the records found in the segment with the given index.
func (wal *WAL) ReadSegmentAt(index int) []record.Record {
	if index < 0 {
		errMsg := fmt.Sprint("index must be greater than or equal to zero, but ", index, " was given.")
		panic(errMsg)
	}
	if index > len(wal.segmentPaths)-1 {
		errMsg := "index is out of bounds."
		panic(errMsg)
	}

	segmentPath := wal.segmentPaths[index]
	return readEntireSegment(segmentPath)
}

// Returns a slice of all the records found in the segments with indices
// in the range [begin, end)
func (wal *WAL) ReadSegmentsInRange(begin int, end int) []record.Record {
	if begin < 0 {
		errMsg := fmt.Sprint("begin must be greater than or equal to zero, but ", begin, " was given.")
		panic(errMsg)
	}
	if end <= 0 {
		errMsg := fmt.Sprint("end must be greater than zero, but ", end, " was given.")
		panic(errMsg)
	}
	if begin >= end {
		errMsg := "begin must be lesser than end."
		panic(errMsg)
	}
	if end > len(wal.segmentPaths) {
		errMsg := "end must be lesser than or equal to the length of the total amount of segments."
		panic(errMsg)
	}

	recs := make([]record.Record, 0)
	for ; begin < end; begin++ {
		segmentPath := wal.segmentPaths[begin]
		recs = append(recs, readEntireSegment(segmentPath)...)
	}

	return recs
}

// Returns a slice of all the records found in all of the segments.
func (wal *WAL) ReadAllSegments() []record.Record {
	recs := make([]record.Record, 0)
	for _, segmentPath := range wal.segmentPaths {
		recs = append(recs, readEntireSegment(segmentPath)...)
	}
	return recs
}

// Returns a slice of all the records found in the segment.
func readEntireSegment(segmentPath string) []record.Record {
	file, err := os.Open(segmentPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		panic(err)
	}
	if stat.Size() == 0 {
		return make([]record.Record, 0)
	}

	mmapf, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		panic(err)
	}
	defer mmapf.Unmap()

	bytesOfRecs := make([]byte, len(mmapf))

	copy(bytesOfRecs, mmapf)

	bytesReader := bytes.NewReader(bytesOfRecs)
	bufferedReader := bufio.NewReader(bytesReader)

	recs := make([]record.Record, 0)
	rec := record.Record{}
	for eof := rec.Deserialize(bufferedReader); !eof; eof = rec.Deserialize(bufferedReader) {
		recs = append(recs, rec)
	}

	return recs
}

// Removes the old segments from the filesystem (based on the low water mark index) and renames
// the remaining ones so they reflect the new state.
func (wal *WAL) DeleteOldSegments() {
	if len(wal.segmentPaths)-1 <= wal.lowWaterMarkIndex {
		return
	}

	segmentsForDeletion := wal.segmentPaths[:wal.lowWaterMarkIndex]
	for _, segmentPath := range segmentsForDeletion {
		err := os.Remove(segmentPath)
		if err != nil {
			panic(err)
		}
	}

	wal.segmentPaths = wal.segmentPaths[wal.lowWaterMarkIndex:]
	for i, oldPath := range wal.segmentPaths {
		newPath := filename.Log(wal.walPath, wal.dbname, i)

		err := os.Rename(oldPath, newPath)
		if err != nil {
			panic(err)
		}

		wal.segmentPaths[i] = newPath
	}

	wal.lastSegmentPath = wal.segmentPaths[len(wal.segmentPaths)-1]
	wal.lastSegmentNumOfRecords = calculateNumOfRecordsInSegment(wal.lastSegmentPath)
}

// Removes all the segments from the filesystem. This should be called after flushing the memtable.
// Note that this will leave one (emptied) segment, preparing the WAL for new appends.
func (wal *WAL) DeleteAllSegments() {
	for _, segmentPath := range wal.segmentPaths {
		err := os.Remove(segmentPath)
		if err != nil {
			panic(err)
		}
	}
	wal.segmentPaths = wal.segmentPaths[:0]

	newSegmentPath := filename.Log(wal.walPath, wal.dbname, 0)
	file, err := os.Create(newSegmentPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	wal.segmentPaths = append(wal.segmentPaths, newSegmentPath)
	wal.lastSegmentPath = newSegmentPath
	wal.lastSegmentNumOfRecords = 0
}

// Truncates the last segment, and sets its number of records to zero.
func (wal *WAL) ResetLastSegment() {
	err := os.Truncate(wal.lastSegmentPath, 0)
	if err != nil {
		panic(err)
	}

	wal.lastSegmentNumOfRecords = 0
}
