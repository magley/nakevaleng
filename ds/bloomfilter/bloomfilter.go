package bloomfilter

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/spaolacci/murmur3"
	"hash"
	"io"
	"math"
	"os"
	"time"
)

func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

func CreateHashFunctions(k uint) ([]hash.Hash32, []uint32) {
	// returns hash functions and seeds
	var h []hash.Hash32
	ts := uint32(time.Now().UnixNano())
	var timestamps []uint32
	for i := uint(0); i < k; i++ {
		// original is prob wrong:
		//h = append(h, murmur3.New32WithSeed(uint32(ts+1)))
		h = append(h, murmur3.New32WithSeed(ts))
		timestamps = append(timestamps, ts)
		ts = uint32(time.Now().UnixNano())
	}
	return h, timestamps
}

type BloomFilter struct {
	// M is number of bits, K is number of hash functions
	M, K uint
	// can this be serialized? not easily
	hashes   []hash.Hash32
	HashSeeds []uint32
	Contents []byte
}

func New(expectedElements int, falsePositiveRate float64) *BloomFilter {
	m := CalculateM(expectedElements, falsePositiveRate)
	k := CalculateK(expectedElements, m)
	hashes, seeds := CreateHashFunctions(k)
	return &BloomFilter{m, k, hashes, seeds,
		make([]byte, int(math.Ceil(float64(m) / 8)))}
}

func (bf *BloomFilter) Add(adding []byte) {
	for _, v := range bf.hashes {
		_, err := v.Write(adding)
		if err != nil {
			panic(err)
		}
		index := v.Sum32() % uint32(bf.M)
		v.Reset()
		byteIndex := index / 8
		inByteIndex := index - byteIndex * 8
		mask := byte(1) << inByteIndex
		bf.Contents[byteIndex] = bf.Contents[byteIndex] | mask
	}
}

func (bf *BloomFilter) Check(checking []byte) bool {
	// returns false if not in set, true if it might be in set
	for _, v := range bf.hashes {
		_, err := v.Write(checking)
		if err != nil {
			panic(err)
		}
		index := v.Sum32() % uint32(bf.M)
		v.Reset()
		byteIndex := index / 8
		inByteIndex := index - byteIndex * 8
		mask := byte(1) << inByteIndex
		if bf.Contents[byteIndex] & mask == 0 {
			return false
		}
	}
	return true
}

func DecodeFromFile(fName string) *BloomFilter {
	reader, err := os.Open(fName)
	if err != nil {
		// todo: add proper err handling
		return nil
	}
    return Decode(reader)
}

func DecodeFromBytes(data []byte) *BloomFilter {
	reader := bytes.NewReader(data)
	return Decode(reader)
}

func Decode(reader io.Reader) *BloomFilter {
	// assumes good input
	decoder := gob.NewDecoder(reader)
	bf := &BloomFilter{}
	err := decoder.Decode(bf)
	if err != nil && err != io.EOF {
		// todo: maybe change err handling
		panic(err)
	}
	bf.hashes = make([]hash.Hash32, len(bf.HashSeeds))
	for i, seed := range bf.HashSeeds {
		bf.hashes[i] = murmur3.New32WithSeed(seed)
	}
	return bf
}

func (bf *BloomFilter) EncodeToFile(fName string) bool {
	// true if managed to finish
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
		return false
	}
	return true
}

func (bf *BloomFilter) EncodeToBytes() []byte {
	// true if managed to finish
	var outBin bytes.Buffer
	encoder := gob.NewEncoder(&outBin)
	err := encoder.Encode(bf)
	encBytes := outBin.Bytes()
	if err != nil {
		return nil
	}
	return encBytes
}

func main() {
	// change package name to main for a quick test
	bf := New(100, 0.2)
	fmt.Println(bf)
	bf.Add([]byte{1, 2})
	bf.Add([]byte{3, 4})
	fmt.Println(bf.Contents)
	bfBytes := bf.EncodeToBytes()
	fmt.Println(bfBytes)
	bf2 := DecodeFromBytes(bfBytes)
	// should be true false true
	fmt.Println(bf2.Check([]byte{1, 2}))
	fmt.Println(bf2.Check([]byte{2, 5}))
	fmt.Println(bf2.Check([]byte{3, 4}))
	fmt.Println(bf2.HashSeeds)
	bf.EncodeToFile("bf31451.bin")
	bf3 := DecodeFromFile("bf31451.bin")
	fmt.Println("---FROM FILE---")
	fmt.Println(bf3.Check([]byte{1, 2}))
	fmt.Println(bf3.Check([]byte{2, 5}))
	fmt.Println(bf3.Check([]byte{3, 4}))
}
