package bloomfilter
//todo: clean c++ remnants from file, update go and create go.sum

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

// todo: add handling in case of invalid index to map
var bfm map[uint32]*BloomFilter
// THE FIRST ELEMENT IS AT 1! not 0!!
var bfCount uint32 = 0
var mapCreated = false

func NewMap() {
    bfm = make(map[uint32]*BloomFilter)
    mapCreated = true
}

func New(expectedElements int, falsePositiveRate float64) uint32 {
    if !mapCreated {
        NewMap()
    }
	m := CalculateM(expectedElements, falsePositiveRate)
	k := CalculateK(expectedElements, m)
	hashes, seeds := CreateHashFunctions(k)
	bf := &BloomFilter{m, k, hashes, seeds,
		make([]byte, int(math.Ceil(float64(m) / 8)))}
	bfCount++
	bfm[bfCount] = bf
	return bfCount
}

func Add(loc uint32, adding []byte) {
    bf := bfm[loc]
    bf.GoAdd(adding)
}

func (bf *BloomFilter) GoAdd(adding []byte) {
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

func Check(loc uint32, checking []byte) bool {
    return bfm[loc].GoCheck(checking)
}

func (bf *BloomFilter) GoCheck(checking []byte) bool {
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

func DecodeFromFile(fName string) int32 {
	reader, err := os.Open(fName)
	if err != nil {
		return -1
	}
    bf := GoDecode(reader)
    if !mapCreated {
        NewMap()
    }
    bfCount++
    bfm[bfCount] = bf
    return int32(bfCount)
}

func DecodeFromBytes(data []byte) uint32 {
	reader := bytes.NewReader(data)
	bf := GoDecode(reader)
	bfCount++
	bfm[bfCount] = bf
	return bfCount
}

func GoDecode(reader io.Reader) *BloomFilter {
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

func EncodeToFile(loc uint32, fName string) bool {
	// true if managed to finish
	if !mapCreated {
		return false
	}
	bf := bfm[loc]
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

func EncodeToBytes(loc uint32, putIn *[]byte) bool {
	// true if managed to finish
	// user should pass putIn of correct size calculated by calc
	if !mapCreated {
		return false
	}
	bf := bfm[loc]
	var outBin bytes.Buffer
	encoder := gob.NewEncoder(&outBin)
	err := encoder.Encode(bf)
	encBytes := outBin.Bytes()
	if len(*putIn) != len(encBytes) {
		return false
	}
	copy(*putIn, encBytes)
	if err != nil {
		return false
	}
	return true
}

func CalcEncodeSize(loc uint32) int32 {
	bf := bfm[loc]
	var outBin bytes.Buffer
	encoder := gob.NewEncoder(&outBin)
	err := encoder.Encode(bf)
	if err != nil {
		return -1
	}
	return int32(outBin.Len())
}

func main() {
	bfNum := New(100, 0.2)
	fmt.Println(bfNum)
	bf := bfm[1]
	fmt.Println(bf)
	bf.GoAdd([]byte{1, 2})
	bf.GoAdd([]byte{3, 4})
	fmt.Println(bf.Contents)
	bfBytes := make([]byte, CalcEncodeSize(1))
	success := EncodeToBytes(1, &bfBytes)
	fmt.Println("Success:", success)
	fmt.Println(bfBytes)
	bf2Num := DecodeFromBytes(bfBytes)
	bf2 := bfm[bf2Num]
	// should be true false true
	fmt.Println(bf2.GoCheck([]byte{1, 2}))
	fmt.Println(bf2.GoCheck([]byte{2, 5}))
	fmt.Println(bf2.GoCheck([]byte{3, 4}))
	fmt.Println(bf2.HashSeeds)
}
