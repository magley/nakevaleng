package wrappereng

import (
	"fmt"
	"nakevaleng/core/record"
	"nakevaleng/ds/cmsketch"
	"nakevaleng/ds/hll"
	coreeng "nakevaleng/engine/core"
	coreconf "nakevaleng/engine/core-config"
)

// the types as kept in records
const (
	TypeVoid           = 0
	TypeCountMinSketch = 1
	TypeHyperLogLog    = 2
)

type WrapperEngine struct {
	core coreeng.CoreEngine
}

func New(conf coreconf.CoreConfig) WrapperEngine {
	return WrapperEngine{*coreeng.New(conf)}
}

func (wen WrapperEngine) PutTyped(user, key string, val []byte, typeInfo byte) bool {
	return wen.core.Put([]byte(user), []byte(key), val, typeInfo)
}

func (wen WrapperEngine) Put(user, key string, val []byte) bool {
	return wen.core.Put([]byte(user), []byte(key), val, TypeVoid)
}

func (wen WrapperEngine) Get(user, key string) (record.Record, bool) {
	return wen.core.Get([]byte(user), []byte(key))
}

func (wen WrapperEngine) Delete(user, key string) bool {
	return wen.core.Delete([]byte(user), []byte(key))
}

func (wen WrapperEngine) PutCMS(user, key string, cms cmsketch.CountMinSketch) bool {
	return wen.PutTyped(user, key, cms.EncodeToBytes(), TypeCountMinSketch)
}

func (wen WrapperEngine) PutHLL(user, key string, hll hll.HLL) bool {
	return wen.PutTyped(user, key, hll.EncodeToBytes(), TypeHyperLogLog)
}

func main() {
	engine := New(coreconf.LoadConfig("conf.yaml"))
	test(engine)
}

func test(engine WrapperEngine) {
	user := "USER"

	// cms testing
	fmt.Println("===CMS===")
	cms := *cmsketch.New(0.1, 0.1)
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
	engine.PutCMS(user, "cs", cms)
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
	hll1 := *hll.New(4)
	hll1.Add([]byte{1, 2})
	hll1.Add([]byte{1, 2})
	hll1.Add([]byte{1, 2})
	hll1.Add([]byte{3, 4, 6})
	hll1.Add([]byte{5, 4, 120})
	fmt.Println(hll1.Estimate())
	fmt.Println("AFTER ENGINEERING")
	engine.PutHLL(user, "hl", hll1)
	rec, found = engine.Get(user, "hl")
	if !found || rec.TypeInfo != TypeHyperLogLog {
		panic(rec)
	}
	hll2 := hll.DecodeFromBytes(rec.Value)
	fmt.Println(hll2.Estimate())
}
