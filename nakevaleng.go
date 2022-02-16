package main

import (
	"fmt"
	"nakevaleng/ds/cmsketch"
	hyperloglog "nakevaleng/ds/hll"
	"nakevaleng/engine/coreconf"
	"nakevaleng/engine/wrappereng"
	"strconv"
)

func main() {
	eng := wrappereng.New(coreconf.LoadConfig("conf.yaml"))
	//test_memtable_threshold(&eng)
	test_cli(&eng)
}

func test_cli(eng *wrappereng.WrapperEngine) {
	// To remove all the debug output that's written on the CLI, search for:
	// fmt.Println("[DBG]\t

	user := "admin"
	badCommand := false

	for true {
		if badCommand {
			fmt.Println()
			fmt.Println("put  [key] [val]   -  insert record")
			fmt.Println("get  [key]         -  find record by key")
			fmt.Println("del  [key]         -  delete record by key")
			fmt.Println("hllc [key] [k]     -  Create HLL object [key] with precision [k] (between 4 and 16)")
			fmt.Println("hll  [key] [val]   -  Put element [val] into HLL [key]")
			fmt.Println("hll  [key]         -  Get estimate for HLL [key]")
			fmt.Println("cmsc [key] [e] [d] -  Create CMS object [key] with epsilon [e] and delta [d] (both 0-1)")
			fmt.Println("cms  [key] [val]   -  Put element [val] into CMS [key]")
			fmt.Println("cmsq [key] [val]   -  Get estimate for element [val] in CMS [key]")
			fmt.Println("quit               -  Exit program")
			badCommand = false
		}
		fmt.Print("\n>")

		cmd, key, val, arg4 := "", "", "", ""
		n, _ := fmt.Scanf("%s %s %s %s\n", &cmd, &key, &val, &arg4)

		if cmd == "quit" {
			break
		} else if cmd == "put" {
			if n != 3 {
				badCommand = true
				continue
			}
			eng.Put(user, key, []byte(val))
			continue
		} else if cmd == "get" {
			if n != 2 {
				badCommand = true
				continue
			}
			rec, found := eng.Get(user, key)
			if !found {
				fmt.Println(key, "not found.")
			} else {
				fmt.Println(rec)
			}
			continue
		} else if cmd == "del" {
			if n != 2 {
				badCommand = true
				continue
			}
			found := eng.Delete(user, key)
			if !found {
				fmt.Println(key, "not found.")
			} else {
				fmt.Println(key, "removed.")
			}
		} else if cmd == "hllc" {
			if n != 3 {
				badCommand = true
				continue
			}
			precision, err := strconv.Atoi(val)
			if err != nil {
				badCommand = true
				continue
			}
			if precision > hyperloglog.HLL_MAX_PRECISION || precision < hyperloglog.HLL_MIN_PRECISION {
				badCommand = true
				continue
			}
			hll := hyperloglog.New(precision)
			eng.PutHLL(user, key, *hll)
		} else if cmd == "hll" {
			if n == 3 {
				hll := eng.GetHLL(user, key)
				if hll == nil {
					fmt.Println(key, "not found.")
					continue
				}
				hll.Add([]byte(val))
				eng.PutHLL(user, key, *hll)
			} else if n == 2 {
				hll := eng.GetHLL(user, key)
				if hll == nil {
					fmt.Println(key, "not found.")
					continue
				}
				fmt.Println(key, "estimate:", hll.Estimate())
			} else {
				badCommand = true
				continue
			}
			continue
		} else if cmd == "cmsc" {
			if n != 4 {
				badCommand = true
				continue
			}
			epsilon, err := strconv.ParseFloat(val, 32)
			if err != nil {
				badCommand = true
				continue
			}
			delta, err := strconv.ParseFloat(arg4, 32)
			if err != nil {
				badCommand = true
				continue
			}
			if epsilon < 0 || epsilon > 1 || delta < 0 || delta > 1 {
				badCommand = true
				continue
			}
			cms := cmsketch.New(epsilon, delta)
			eng.PutCMS(user, key, *cms)

		} else if cmd == "cms" {
			if n != 3 {
				badCommand = true
				continue
			}

			cms := eng.GetCMS(user, key)
			if cms == nil {
				fmt.Println(key, "not found.")
				continue
			}
			cms.Insert([]byte(val))
			eng.PutCMS(user, key, *cms)
			continue
		} else if cmd == "cmsq" {
			if n != 3 {
				badCommand = true
				continue
			}

			cms := eng.GetCMS(user, key)
			if cms == nil {
				fmt.Println(key, "not found.")
				continue
			}
			fmt.Println(key, "estimate:", cms.Query([]byte(val)))
			continue
		} else {
			badCommand = true
			continue
		}
	}
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
