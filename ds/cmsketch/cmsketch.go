package cmsketch

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
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
	Contents [][]uint
}

func New(epsilon, delta float64) *CountMinSketch {
	m := CalculateM(epsilon)
	k := CalculateK(delta)
	hashes, seeds := CreateHashFunctions(k)
	contents := make([][]uint, k)
	for i := range contents {
		contents[i] = make([]uint, m)
	}
	return &CountMinSketch{m, k, seeds, hashes, contents}
}

func (cms *CountMinSketch) Add(adding []byte) {
	for i, v := range cms.hashes {
		_, err := v.Write(adding)
		if err != nil {
			panic(err)
		}
		index := v.Sum32() % uint32(cms.M)
		v.Reset()
		cms.Contents[i][index] += 1
	}
}

func (cms *CountMinSketch) EstimateFrequency(checking []byte) uint {
	var min uint
	for i, v := range cms.hashes {
		_, err := v.Write(checking)
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
	// todo: test and implement rest
	var outBin bytes.Buffer
	encoder := gob.NewEncoder(&outBin)
	err := encoder.Encode(cms)
	encBytes := outBin.Bytes()
	if err != nil {
		return nil
	}
	return encBytes
}

func main() {
	cms := New(0.1, 0.1)
	fmt.Println(cms.K)
	fmt.Println(cms.M)
	fmt.Println(cms.Contents)
	cms.Add([]byte{1, 2})
	fmt.Println(cms.Contents)
	cms.Add([]byte{1, 2})
	fmt.Println(cms.Contents)
	cms.Add([]byte{3, 4})
	fmt.Println(cms.Contents)
	fmt.Println(cms.EstimateFrequency([]byte{1, 2}))
	fmt.Println(cms.EstimateFrequency([]byte{3, 4}))
	fmt.Println(cms.EstimateFrequency([]byte{2, 5}))
}
