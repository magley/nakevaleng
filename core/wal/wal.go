package wal

import (
	"bufio"
	"bytes"
	"nakevaleng/core/record"
	"nakevaleng/util/filename"
	"os"

	"github.com/edsrzf/mmap-go"
)

// TODO: Make these configurable
const (
	appendingBufferCapacity = 5
	maxRecordsInSegment     = 5
	lowWaterMarkIndex       = 1 // Must be at least 1. Note that the first segment's index is 0
)

type WAL struct {
	segmentFilenames        []string
	lastSegmentFilename     string
	lastSegmentNumOfRecords int
	appendingBuffer         []record.Record
}

func New() *WAL {
	segmentFilenames := filename.GetSegmentFilenames("data/log/", "nakevaleng")
	if len(segmentFilenames) == 0 {
		lastSegmentFilename := filename.Log("data/log/", "nakevaleng", 0)

		_, err := os.Create(lastSegmentFilename)
		if err != nil {
			panic(err)
		}

		segmentFilenames = append(segmentFilenames, lastSegmentFilename)
	}

	lastSegmentFilename := segmentFilenames[len(segmentFilenames)-1]
	lastSegmentNumOfRecords := calculateNumOfRecordsInSegment(lastSegmentFilename)

	buffer := make([]record.Record, 0, appendingBufferCapacity)

	return &WAL{segmentFilenames, lastSegmentFilename, lastSegmentNumOfRecords, buffer}
}

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

func (wal *WAL) Append(rec record.Record) {
	if wal.lastSegmentNumOfRecords == maxRecordsInSegment {
		wal.addSegment() // Append is now operating on the new last segment
	}

	file, err := os.OpenFile(wal.lastSegmentFilename, os.O_RDWR, 0666)
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

func (wal *WAL) BufferedAppend(rec record.Record) {
	wal.appendingBuffer = append(wal.appendingBuffer, rec)
	if len(wal.appendingBuffer) == cap(wal.appendingBuffer) {
		wal.FlushBuffer()
	}
}

func (wal *WAL) FlushBuffer() {
	for len(wal.appendingBuffer) != 0 {
		if wal.lastSegmentNumOfRecords == maxRecordsInSegment {
			wal.addSegment() // Append is now operating on the new last segment
		}

		numOfRecsToFlush := maxRecordsInSegment - wal.lastSegmentNumOfRecords
		if numOfRecsToFlush > len(wal.appendingBuffer) {
			numOfRecsToFlush = len(wal.appendingBuffer)
		}

		partialBuffer := wal.appendingBuffer[:numOfRecsToFlush]
		wal.flushPartialBufferToSegment(partialBuffer)

		wal.appendingBuffer = wal.appendingBuffer[numOfRecsToFlush:]
		wal.lastSegmentNumOfRecords += numOfRecsToFlush
	}
}

func (wal *WAL) addSegment() {
	_, logNo, _, _ := filename.Query(wal.lastSegmentFilename)
	logNo++

	newLastSegmentFilename := filename.Log("data/log/", "nakevaleng", logNo)

	_, err := os.Create(newLastSegmentFilename)
	if err != nil {
		panic(err)
	}

	wal.lastSegmentFilename = newLastSegmentFilename
	wal.lastSegmentNumOfRecords = 0
}

func (wal *WAL) flushPartialBufferToSegment(partialBuffer []record.Record) {
	file, err := os.OpenFile(wal.lastSegmentFilename, os.O_RDWR, 0666)
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

func (wal *WAL) ReadLastSegment() []record.Record {
	return wal.readEntireSegment(wal.lastSegmentFilename)
}

// Do we need this? What would it even do?
// func (wal *WAL) ReadRange(begin int, end int) {

// }

func (wal *WAL) ReadAllSegments() []record.Record {
	recs := make([]record.Record, 0)
	for _, segmentFilename := range wal.segmentFilenames {
		recs = append(recs, wal.readEntireSegment(segmentFilename)...)
	}
	return recs
}

func (wal *WAL) readEntireSegment(segmentFilename string) []record.Record {
	file, err := os.Open(segmentFilename)
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

func (wal *WAL) DeleteOldSegments() {
	if len(wal.segmentFilenames)-1 <= lowWaterMarkIndex {
		return
	}

	segmentsForDeletion := wal.segmentFilenames[:lowWaterMarkIndex]
	for _, segmentFilename := range segmentsForDeletion {
		err := os.Remove(segmentFilename)
		if err != nil {
			panic(err)
		}
	}

	wal.segmentFilenames = wal.segmentFilenames[lowWaterMarkIndex:]
	for i, oldFilename := range wal.segmentFilenames {
		newFilename := filename.Log("data/log/", "nakevaleng", i)

		err := os.Rename(oldFilename, newFilename)
		if err != nil {
			panic(err)
		}

		wal.segmentFilenames[i] = newFilename
	}

	wal.lastSegmentFilename = wal.segmentFilenames[len(wal.segmentFilenames)-1]
	wal.lastSegmentNumOfRecords = calculateNumOfRecordsInSegment(wal.lastSegmentFilename)
}

func (wal *WAL) ResetLastSegment() {
	err := os.Truncate(wal.lastSegmentFilename, 0)
	if err != nil {
		panic(err)
	}

	wal.lastSegmentNumOfRecords = 0
}
