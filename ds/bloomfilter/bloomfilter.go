package bloomfilter

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"time"

	"github.com/spaolacci/murmur3"
)

func CalculateM(expectedElements int, falsePositiveRate float64) uint32 {
	return uint32(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements int, m uint32) uint32 {
	return uint32(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

// CreateHashFunctions creates k-many hash functions.
// Returns the hash functions and their timestamps (seeds).
func CreateHashFunctions(k uint32) ([]hash.Hash32, []uint32) {
	var hashes []hash.Hash32
	var timestamps []uint32
	t := uint32(time.Now().UnixNano())

	for i := uint32(0); i < k; i++ {
		timestamps = append(timestamps, t+i)
		hashes = append(hashes, murmur3.New32WithSeed(t+i))
	}

	return hashes, timestamps
}

type BloomFilter struct {
	M         uint32        // Number of bits
	K         uint32        // Number of hash functions
	HashSeeds []uint32      // All the seeds
	Contents  []byte        // Actual data, all the bits are used for each byte, therefore: contents size * 8  ==  M
	hashes    []hash.Hash32 // Hash functions
}

// New Creates a new BloomFilter object.
//		expectedElements :: "Number of elements likely to be inserted during the bloom filter's lifetime"
//		falsePositiveRate :: "Probability of error when querying, (0 to 1), if unsure use 0.1"
func New(expectedElements int, falsePositiveRate float64) *BloomFilter {
	m := CalculateM(expectedElements, falsePositiveRate)
	k := CalculateK(expectedElements, m)
	hashes, seeds := CreateHashFunctions(k)

	return &BloomFilter{
		M:         m,
		K:         k,
		HashSeeds: seeds,
		Contents:  make([]byte, int(math.Ceil(float64(m)/8))),
		hashes:    hashes,
	}
}

// Insert a new element (byte sequence) into the bloom filter.
func (bf *BloomFilter) Insert(element []byte) {
	for _, v := range bf.hashes {
		_, err := v.Write(element)
		if err != nil {
			panic(err)
		}
		index := v.Sum32() % bf.M
		v.Reset()

		byteIndex := index / 8
		bitIndex := index % 8
		mask := byte(1 << bitIndex)
		bf.Contents[byteIndex] |= mask
	}
}

// Query element for presence in set. Returns:
//		false :: "true negative (element is NOT in the set)"
//		true  :: "positive (element may be in the set)"
func (bf *BloomFilter) Query(element []byte) bool {
	for _, v := range bf.hashes {
		_, err := v.Write(element)
		if err != nil {
			panic(err)
		}
		index := v.Sum32() % bf.M
		v.Reset()

		byteIndex := index / 8
		bitIndex := index % 8
		mask := byte(1 << bitIndex)

		if bf.Contents[byteIndex]&mask == 0 {
			return false
		}
	}
	return true
}

// DecodeFromFile reads from file.
func DecodeFromFile(filename string) *BloomFilter {
	reader, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer func(reader *os.File) {
		err := reader.Close()
		if err != nil {
			panic(err)
		}
	}(reader)
	return decode(reader)
}

// DecodeFromBytes reads from bytes.
func DecodeFromBytes(data []byte) *BloomFilter {
	reader := bytes.NewReader(data)
	return decode(reader)
}

// Read from decoder. Uses gob encoding.
//
func decode(reader io.Reader) *BloomFilter {
	// Read all data into the filter.

	decoder := gob.NewDecoder(reader)
	bf := &BloomFilter{}
	err := decoder.Decode(bf)
	if err != nil && err != io.EOF {

		panic(err)
	}

	// Make hashes.

	bf.hashes = make([]hash.Hash32, len(bf.HashSeeds))
	for i, seed := range bf.HashSeeds {
		bf.hashes[i] = murmur3.New32WithSeed(seed)
	}

	return bf
}

// EncodeToFile writes bloom filter data into a file. Uses gob encoding.
func (bf *BloomFilter) EncodeToFile(fName string) {
	outBin, _ := os.Create(fName)
	defer func(outBin *os.File) {
		err := outBin.Close()
		if err != nil {
			panic(err)
		}
	}(outBin)

	encoder := gob.NewEncoder(outBin)
	err := encoder.Encode(bf)

	if err != nil {
		panic(err)
	}
}

// EncodeToBytes writes bloom filter data into a sequence of bytes.
// Returns the byte sequence if successful
func (bf *BloomFilter) EncodeToBytes() []byte {
	var outBin bytes.Buffer
	encoder := gob.NewEncoder(&outBin)
	err := encoder.Encode(bf)
	encBytes := outBin.Bytes()
	if err != nil {
		panic(err)
	}

	return encBytes
}

func main() {
	// change package name to main for a quick test
	bf := New(100, 0.2)
	fmt.Println(bf)
	bf.Insert([]byte{1, 2})
	bf.Insert([]byte{3, 4})
	fmt.Println(bf.Contents)
	bfBytes := bf.EncodeToBytes()
	fmt.Println(bfBytes)
	bf2 := DecodeFromBytes(bfBytes)
	// should be true false true
	fmt.Println(bf2.Query([]byte{1, 2}))
	fmt.Println(bf2.Query([]byte{2, 5}))
	fmt.Println(bf2.Query([]byte{3, 4}))
	fmt.Println(bf2.HashSeeds)
	bf.EncodeToFile("bf31451.bin")
	bf3 := DecodeFromFile("bf31451.bin")
	fmt.Println("---FROM FILE---")
	fmt.Println(bf3.Query([]byte{1, 2}))
	fmt.Println(bf3.Query([]byte{2, 5}))
	fmt.Println(bf3.Query([]byte{3, 4}))
}
