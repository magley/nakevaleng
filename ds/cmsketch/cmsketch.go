package cmsketch

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

func CalculateM(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

func CalculateK(delta float64) uint {
	// wrong formula???? the wrong one always gives k one number larger than
	// the right one
	//return uint(math.Ceil(math.Log(math.E / delta)))
	return uint(math.Ceil(math.Log(1 / delta)))
}

func CreateHashFunctions(k uint) ([]hash.Hash32, []uint32) {
	var h []hash.Hash32
	var timestamps []uint32
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(ts+i)))
		timestamps = append(timestamps, uint32(ts + i))
	}
	return h, timestamps
}

type CountMinSketch struct {
	// K is number of hashes, M number of cols
	M, K uint
	HashSeeds []uint32
	hashes []hash.Hash32
	Contents [][]uint32
}

func New(epsilon, delta float64) *CountMinSketch {
	m := CalculateM(epsilon)
	k := CalculateK(delta)
	hashes, seeds := CreateHashFunctions(k)
	contents := make([][]uint32, k)
	for i := range contents {
		contents[i] = make([]uint32, m)
	}
	return &CountMinSketch{m, k, seeds, hashes, contents}
}

func (cms *CountMinSketch) Add(element []byte) {
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

// EstimateFrequency of element in a stream of data
func (cms *CountMinSketch) EstimateFrequency(element []byte) uint32 {
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

func (cms *CountMinSketch) EncodeToBytes() []byte {
	var outBin bytes.Buffer
	encoder := gob.NewEncoder(&outBin)
	err := encoder.Encode(cms)
	if err != nil {
		panic(err)
	}
	encBytes := outBin.Bytes()
	return encBytes
}

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

func Decode(reader io.Reader) *CountMinSketch {
	decoder := gob.NewDecoder(reader)
	cms := &CountMinSketch{}
	err := decoder.Decode(cms)
	if err != nil {
		panic(err)
	}
	cms.hashes = make([]hash.Hash32, len(cms.HashSeeds))
	for i, seed := range cms.HashSeeds {
		cms.hashes[i] = murmur3.New32WithSeed(seed)
	}
	return cms
}

func DecodeFromBytes(data []byte) *CountMinSketch {
	reader := bytes.NewReader(data)
	return Decode(reader)
}

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
	return Decode(reader)
}

func main() {
	cms := New(0.1, 0.1)
	fmt.Println(cms.K)
	fmt.Println(cms.M)
	cms.Add([]byte{1, 2})
	cms.Add([]byte{1, 2})
	cms.Add([]byte{3, 4})
	fmt.Println(cms.Contents)
	fmt.Println(cms.EstimateFrequency([]byte{1, 2}))
	fmt.Println(cms.EstimateFrequency([]byte{3, 4}))
	fmt.Println(cms.EstimateFrequency([]byte{2, 5}))
	cmsBytes := cms.EncodeToBytes()
	cms2 := DecodeFromBytes(cmsBytes)
	fmt.Println(cms2.EstimateFrequency([]byte{1, 2}))
	fmt.Println(cms2.EstimateFrequency([]byte{3, 4}))
	fmt.Println(cms2.EstimateFrequency([]byte{2, 5}))
	cms2.EncodeToFile("cms31451.bin")
	cms3 := DecodeFromFile("cms31451.bin")
	fmt.Println(cms3.EstimateFrequency([]byte{1, 2}))
	fmt.Println(cms3.EstimateFrequency([]byte{3, 4}))
	fmt.Println(cms3.EstimateFrequency([]byte{2, 5}))
	fmt.Println(cms3.Contents)
}
