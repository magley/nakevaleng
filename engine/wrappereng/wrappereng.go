// Package wrappereng implements a WrapperEngine used for convenient wrapping around
// the more complex CoreEngine.
package wrappereng

import (
	"fmt"
	"nakevaleng/core/record"
	"nakevaleng/ds/cmsketch"
	"nakevaleng/ds/hll"
	"nakevaleng/engine/coreconf"
	"nakevaleng/engine/coreeng"
)

// The types as kept in records.
const (
	TypeVoid           = 0
	TypeCountMinSketch = 1
	TypeHyperLogLog    = 2
)

// WrapperEngine is a thin application layer wrapping around CoreEngine, with additional support for
// easy reading and writing of CMS and HLL objects.
type WrapperEngine struct {
	core coreeng.CoreEngine
}

// New returns a new WrapperEngine object.
func New(conf *coreconf.CoreConfig) WrapperEngine {
	cen, _ := coreeng.New(conf)

	return WrapperEngine{*cen}
}

// PutTyped writes a new record in the system based on the passed key, val and typeInfo
// parameters.
func (wen WrapperEngine) PutTyped(user, key string, val []byte, typeInfo byte) bool {
	return wen.core.Put([]byte(user), []byte(key), val, typeInfo)
}

// Put writes a new record in the system based on the passed key and val parameters.
func (wen WrapperEngine) Put(user, key string, val []byte) bool {
	return wen.core.Put([]byte(user), []byte(key), val, TypeVoid)
}

// Get returns a record stored in the system based on the passed key, as well as
// whether or not the record is present.
func (wen WrapperEngine) Get(user, key string) (record.Record, bool) {
	return wen.core.Get([]byte(user), []byte(key))
}

// Delete does logical deletion of the record with the passed key in the system
// (if it exists). Returns whether or not the deletion was successful.
func (wen WrapperEngine) Delete(user, key string) bool {
	return wen.core.Delete([]byte(user), []byte(key))
}

// PutCMS writes a new record in the system whose value represents a CMS object.
func (wen WrapperEngine) PutCMS(user, key string, cms cmsketch.CountMinSketch) bool {
	return wen.PutTyped(user, key, cms.EncodeToBytes(), TypeCountMinSketch)
}

// PutHLL writes a new record in the system whose value represents a HLL object.
func (wen WrapperEngine) PutHLL(user, key string, hll hll.HLL) bool {
	return wen.PutTyped(user, key, hll.EncodeToBytes(), TypeHyperLogLog)
}

// GetCMS returns a CountMinSketch object found in the system under the passed key.
func (wen WrapperEngine) GetCMS(user, key string) *cmsketch.CountMinSketch {
	rec, found := wen.Get(user, key)
	if !found || rec.TypeInfo != TypeCountMinSketch {
		return nil
	}
	return cmsketch.DecodeFromBytes(rec.Value)
}

// GetHLL returns a HyperLogLog object found in the system under the passed key.
func (wen WrapperEngine) GetHLL(user, key string) *hll.HLL {
	rec, found := wen.Get(user, key)
	if !found || rec.TypeInfo != TypeHyperLogLog {
		return nil
	}
	return hll.DecodeFromBytes(rec.Value)
}

// FlushWALBuffer is a convenience function for flushing the WAL's buffer.
func (wen WrapperEngine) FlushWALBuffer() {
	wen.core.FlushWALBuffer()
}

func main() {
	conf, err := coreconf.LoadConfig("conf.yaml")
	if err != nil {
		panic(err)
	}

	engine := New(conf)
	test(engine)
}

func test(engine WrapperEngine) {
	user := "USER"

	// cms testing
	fmt.Println("===CMS===")
	cms, _ := cmsketch.New(0.1, 0.1)
	fmt.Println(cms.K)
	fmt.Println(cms.M)
	cms.Insert([]byte{1, 2})
	cms.Insert([]byte{1, 2})
	cms.Insert([]byte{3, 4})
	fmt.Println(cms.Contents)
	fmt.Println(cms.Query([]byte{1, 2}))
	fmt.Println(cms.Query([]byte{3, 4}))
	fmt.Println(cms.Query([]byte{2, 5}))
	fmt.Println("AFTER ENGINEERING")
	engine.PutCMS(user, "cs", *cms)
	rec, found := engine.Get(user, "cs")
	if !found || rec.TypeInfo != TypeCountMinSketch {
		panic(rec)
	}
	cms2 := cmsketch.DecodeFromBytes(rec.Value)
	fmt.Println(cms2.K)
	fmt.Println(cms2.M)
	//cms2.Insert([]byte{1, 2})
	//cms2.Insert([]byte{1, 2})
	//cms2.Insert([]byte{3, 4})
	fmt.Println(cms2.Contents)
	fmt.Println(cms2.Query([]byte{1, 2}))
	fmt.Println(cms2.Query([]byte{3, 4}))
	fmt.Println(cms2.Query([]byte{2, 5}))

	// hll testing
	fmt.Println("\n===HLL===")
	hll1, _ := hll.New(4)
	hll1.Add([]byte{1, 2})
	hll1.Add([]byte{1, 2})
	hll1.Add([]byte{1, 2})
	hll1.Add([]byte{3, 4, 6})
	hll1.Add([]byte{5, 4, 120})
	fmt.Println(hll1.Estimate())
	fmt.Println("AFTER ENGINEERING")
	engine.PutHLL(user, "hl", *hll1)
	rec, found = engine.Get(user, "hl")
	if !found || rec.TypeInfo != TypeHyperLogLog {
		panic(rec)
	}
	hll2 := hll.DecodeFromBytes(rec.Value)
	fmt.Println(hll2.Estimate())
	engine.FlushWALBuffer()
}
