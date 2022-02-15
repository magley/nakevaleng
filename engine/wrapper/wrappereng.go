package main

import (
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
	// todo
}
