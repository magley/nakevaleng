package main

import (
	"nakevaleng/engine/coreconf"
	"nakevaleng/engine/wrappereng"
)

func main() {
	eng := wrappereng.New(coreconf.LoadConfig("conf.yaml"))
	test_memtable_threshold(&eng)
}

func test_memtable_threshold(eng *wrappereng.WrapperEngine) {
	// 3 elements (way less than memtable capacity)
	// But in total they take up 3000 B
	// If memtable threshold is 2000 B, then a flush will happen.
	// Token buckets are also written though.
	// CMD output is written in memtable.go @ line 39

	val1 := make([]byte, 1000)
	val2 := make([]byte, 1000)
	val3 := make([]byte, 1000)
	user := "admin"

	eng.Put(user, "key1", val1)
	eng.Put(user, "key2", val2)
	eng.Put(user, "key3", val3)
}
