package cmsketch

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

func CalculateM(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

func CalculateK(delta float64) uint {
	return uint(math.Ceil(math.Log(1 / delta)))
}

// CreateHashFunctions creates k-many hash functions.
// Returns the hash functions and their timestamps (seeds).
func CreateHashFunctions(k uint) ([]hash.Hash32, []uint32) {
	var hashes []hash.Hash32
	var timestamps []uint32
	t := uint(time.Now().Unix())

	for i := uint(0); i < k; i++ {
		timestamps = append(timestamps, uint32(t+i))
		hashes = append(hashes, murmur3.New32WithSeed(uint32(t+i)))
	}

	return hashes, timestamps
}

type CountMinSketch struct {
	M         uint          // Number of columns in the table
	K         uint          // Number of hashes (and rows in the table)
	HashSeeds []uint32      // Seeds for each of the K functions
	Contents  [][]uint32    // Actual table
	hashes    []hash.Hash32 // Hash functions
}

// New creates a new CountMinSketch object
//		epsilon :: "rate of imprecision (0, 1), if unsure use 0.1"
//		delta	:: "rate of error (0, 1), if unsure use 0.1"
func New(epsilon, delta float64) *CountMinSketch {
	m := CalculateM(epsilon)
	k := CalculateK(delta)
	hashes, seeds := CreateHashFunctions(k)

	contents := make([][]uint32, k)
	for i := range contents {
		contents[i] = make([]uint32, m)
	}

	return &CountMinSketch{m, k, seeds, contents, hashes}
}

// Insert a new element (byte sequence) into the CMS.
func (cms *CountMinSketch) Insert(element []byte) {
	for i, v := range cms.hashes {
		_, err := v.Write(element)
		if err != nil {
			panic(err)
		}
		index := v.Sum32() % uint32(cms.M)
		v.Reset()

		cms.Contents[i][index] += 1
	}
}

// Query estimates the frequency of the element in CMS.
// Returns an integer estimation.
func (cms *CountMinSketch) Query(element []byte) uint32 {
	var min uint32
	for i, v := range cms.hashes {
		_, err := v.Write(element)
		if err != nil {
			panic(err)
		}
		index := v.Sum32() % uint32(cms.M)
		v.Reset()

		tmp := cms.Contents[i][index]

		if i == 0 {
			min = tmp
		} else if tmp < min {
			min = tmp
		}
	}

	return min
}

// EncodeToBytes writes CMS data into a sequence of bytes.
// Returns the byte sequence if successful.
func (cms *CountMinSketch) EncodeToBytes() []byte {
	var outBin bytes.Buffer
	encoder := gob.NewEncoder(&outBin)
	err := encoder.Encode(cms)
	encBytes := outBin.Bytes()
	if err != nil {
		panic(err)
	}

	return encBytes
}

// EncodeToFile writes CMS data into a file. Uses gob encoding.
func (cms *CountMinSketch) EncodeToFile(filename string) {
	outBin, err := os.Create(filename)
	if err != nil {
		panic(err)
	}

	defer func(outBin *os.File) {
		err := outBin.Close()
		if err != nil {
			panic(err)
		}
	}(outBin)

	encoder := gob.NewEncoder(outBin)
	err = encoder.Encode(cms)

	if err != nil {
		panic(err)
	}
}

// Decode from reader.
// Uses gob encoding.
func decode(reader io.Reader) *CountMinSketch {
	// Read all data into the CMS.

	decoder := gob.NewDecoder(reader)
	cms := &CountMinSketch{}
	err := decoder.Decode(cms)
	if err != nil && err != io.EOF {
		panic(err)
	}

	// Make hashes (because we don't serialize those, only their timestamps)

	cms.hashes = make([]hash.Hash32, len(cms.HashSeeds))
	for i, seed := range cms.HashSeeds {
		cms.hashes[i] = murmur3.New32WithSeed(seed)
	}

	return cms
}

// DecodeFromBytes reads data from a byte stream and writes into a new CMS.
// Uses gob encoding.
func DecodeFromBytes(data []byte) *CountMinSketch {
	reader := bytes.NewReader(data)
	return decode(reader)
}

// DecodeFromBytes reads data from a file and writes into a new CMS.
// Uses gob encoding.
func DecodeFromFile(filename string) *CountMinSketch {
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

func main() {
	cms := New(0.1, 0.1)
	fmt.Println(cms.K)
	fmt.Println(cms.M)
	cms.Insert([]byte{1, 2})
	cms.Insert([]byte{1, 2})
	cms.Insert([]byte{3, 4})
	fmt.Println(cms.Contents)
	fmt.Println(cms.Query([]byte{1, 2}))
	fmt.Println(cms.Query([]byte{3, 4}))
	fmt.Println(cms.Query([]byte{2, 5}))
	cmsBytes := cms.EncodeToBytes()
	cms2 := DecodeFromBytes(cmsBytes)
	fmt.Println(cms2.Query([]byte{1, 2}))
	fmt.Println(cms2.Query([]byte{3, 4}))
	fmt.Println(cms2.Query([]byte{2, 5}))
	cms2.EncodeToFile("cms31451.bin")
	cms3 := DecodeFromFile("cms31451.bin")
	fmt.Println(cms3.Query([]byte{1, 2}))
	fmt.Println(cms3.Query([]byte{3, 4}))
	fmt.Println(cms3.Query([]byte{2, 5}))
	fmt.Println(cms3.Contents)
}
