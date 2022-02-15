package hll

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"

	"github.com/spaolacci/murmur3"
)

// Keep these constants here?
const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

type HLL struct {
	m   uint64
	p   uint8
	reg []uint8
}

// Returns a pointer to a new HLL object.
func New(precision uint8) *HLL {
	if precision < HLL_MIN_PRECISION || precision > HLL_MAX_PRECISION {
		errMsg := fmt.Sprint("precision must be between ", HLL_MIN_PRECISION, " and ", HLL_MAX_PRECISION, ", but ", precision, " was given.")
		panic(errMsg)
	}

	m := uint64(math.Pow(2, float64(precision)))
	reg := make([]uint8, m)

	return &HLL{m, precision, reg}
}

func (hll *HLL) Add(data []byte) {
	// hash the data
	hash := murmur3.New32()
	hash.Write(data)

	// represent it as an integer
	i := hash.Sum32()

	// get the register index from the first p bytes of i
	idx := i >> uint32(32-hll.p)

	// get the number of trailing zeroes + 1 from i's bytes
	val := uint8(1 + bits.TrailingZeros32(i))

	// if val is greater than the value already present in the register,
	// then put val in instead.
	if hll.reg[idx] < val {
		hll.reg[idx] = val
	}
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum = sum + math.Pow(float64(-val), 2.0)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation < 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > math.Pow(2.0, 32.0)/30.0 { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

// Reads data from a file and returns a hyperloglog generated by it.
// Uses gob encoding.
func DecodeFromFile(filename string) *HLL {
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	return decode(f)
}

// Reads data from a byte sequence and returns a hyperloglog generated by it.
// Uses gob encoding.
func DecodeFromBytes(data []byte) *HLL {
	reader := bytes.NewReader(data)
	return decode(reader)
}

// Read from decoder. Uses gob encoding.
func decode(reader io.Reader) *HLL {
	// Read all data into the filter.

	decoder := gob.NewDecoder(reader)
	hll := &HLL{}
	err := decoder.Decode(hll)
	if err != nil && err != io.EOF {
		panic(err)
	}

	return hll
}

// Writes hyperloglog data into a file.
// Uses gob encoding.
func (hll *HLL) EncodeToFile(fName string) {
	outBin, err := os.Create(fName)
	if err != nil {
		panic(err)
	}
	defer outBin.Close()

	encoder := gob.NewEncoder(outBin)
	err = encoder.Encode(hll)
	if err != nil {
		panic(err)
	}
}

// Writes hyperloglog data into a sequence of bytes.
// Returns the byte sequence if successful.
// Uses gob encoding.
func (hll *HLL) EncodeToBytes() []byte {
	outBin := bytes.Buffer{}
	encoder := gob.NewEncoder(&outBin)
	err := encoder.Encode(hll)
	if err != nil {
		panic(err)
	}
	encBytes := outBin.Bytes()

	return encBytes
}