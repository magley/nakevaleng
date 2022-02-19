// Package wrappertest implements functions used for testing the system.
package wrappertest

import (
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"nakevaleng/core/record"
	"nakevaleng/ds/cmsketch"
	"nakevaleng/ds/hll"
	"nakevaleng/engine/coreconf"
	"nakevaleng/engine/wrappereng"
	"os"
	"strconv"
)

// Test reads the generated test file and parses it into commands to be
// used by the WrapperEngine.
func Test(wen wrappereng.WrapperEngine, testPath string) {
	f, err := os.OpenFile(testPath, os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	csvReader := csv.NewReader(f)
	csvReader.FieldsPerRecord = -1
	_, err = csvReader.Read()
	if err != nil {
		panic(err)
	}
	var got record.Record
	var found bool
	for {
		rec, err := csvReader.Read()
		if rec == nil {
			break
		}
		if err != nil {
			panic(err)
		}
		if rec[1] == "P" {
			if rec[3] == "HLL" {
				p, err := strconv.Atoi(rec[4])
				if err != nil {
					panic(err)
				}
				hl, err := hll.New(p)
				if err != nil {
					panic(err)
				}
				for j := 5; j < len(rec); j++ {
					v, err := strconv.Atoi(rec[j])
					if err != nil {
						panic(err)
					}
					hl.Add([]byte{byte(v)})
				}
				wen.PutHLL(rec[0], rec[2], *hl)
			} else if rec[3] == "CMS" {
				epsilon, err := strconv.ParseFloat(rec[4], 64)
				if err != nil {
					panic(err)
				}
				delta, err := strconv.ParseFloat(rec[5], 64)
				if err != nil {
					panic(err)
				}
				cms, err := cmsketch.New(epsilon, delta)
				if err != nil {
					panic(err)
				}
				for j := 6; j < len(rec); j++ {
					v, err := strconv.Atoi(rec[j])
					if err != nil {
						panic(err)
					}
					cms.Insert([]byte{byte(v)})
				}
				wen.PutCMS(rec[0], rec[2], *cms)
			} else {
				wen.Put(rec[0], rec[2], []byte(rec[3]))
			}
		} else if rec[1] == "D" {
			wen.Delete(rec[0], rec[2])
		} else if rec[1] == "G" {
			got, found = wen.Get(rec[0], rec[2])
			if found {
				if got.TypeInfo == wrappereng.TypeHyperLogLog {
					fmt.Println(rec[2], "returned HLL with estimate at:", hll.DecodeFromBytes(got.Value).Estimate())
				} else if got.TypeInfo == wrappereng.TypeCountMinSketch {
					fmt.Println(rec[2], "returned CMS with 3 presence estimate at:",
						cmsketch.DecodeFromBytes(got.Value).Query([]byte("3")))
				} else if got.TypeInfo == wrappereng.TypeVoid {
					fmt.Println(rec[2], "returns:", got)
				} else {
					log.Panicln("UNEXPECTED TYPE: ", got)
				}
			} else {
				fmt.Println(rec[2], "not found")
			}
		}
	}
	wen.FlushWALBuffer()
}

// GenerateTest creates a CSV file with randomly generated commands to be
// passed to the WrapperEngine in Test.
func GenerateTest(testPath string, commands int, maxLen, hllMaxLen, cmsMaxLen int) {
	f, err := os.Create(testPath)
	if err != nil {
		panic(err)
	}
	csvWriter := csv.NewWriter(f)
	err = csvWriter.Write([]string{"#username", "command", "key", "val"})
	if err != nil {
		panic(err)
	}
	var username, command string
	var key, val []byte
	var presentKeys []string
	for i := 0; i < commands; i++ {
		rec := make([]string, 0)
		k := rand.Intn(3)
		if k == 0 {
			username = "USER0000"
		} else if k == 1 {
			username = "USER0001"
		} else {
			username = "USER0002"
		}
		rec = append(rec, username)

		k = rand.Intn(100)
		if k < 60 {
			command = "P"
		} else if k < 94 {
			command = "G"
		} else {
			command = "D"
		}
		rec = append(rec, command)

		// select key
		k = rand.Intn(8)
		if k <= 5 || len(presentKeys) == 0 {
			key = make([]byte, rand.Intn(maxLen)+1)
			for j := 0; j < len(key); j++ {
				key[j] = byte(65 + rand.Intn(26))
			}
			presentKeys = append(presentKeys, string(key))
		} else {
			key = []byte(presentKeys[rand.Intn(len(presentKeys))])
		}
		rec = append(rec, string(key))

		// determine what to put if command is put
		if command == "P" {
			k = rand.Intn(100)
			if k < 50 {
				// type,precision
				tmp := []string{"HLL", "8"}
				bound := rand.Intn(hllMaxLen) + 1
				for j := 0; j < bound; j++ {
					tmp = append(tmp, strconv.Itoa(rand.Intn(10)))
				}
				rec = append(rec, tmp...)
			} else if k < 70 {
				tmp := []string{"CMS", "0.1", "0.1"}
				bound := rand.Intn(cmsMaxLen) + 1
				for j := 0; j < bound; j++ {
					tmp = append(tmp, strconv.Itoa(rand.Intn(10)))
				}
				rec = append(rec, tmp...)
			} else {
				val = make([]byte, rand.Intn(maxLen)+1)
				for j := 0; j < len(val); j++ {
					val[j] = byte(65 + rand.Intn(26))
				}
				// just in case
				if string(val) == "CMS" || string(val) == "HLL" {
					val = append(val, []byte("_WHAT_ARE_THE_ODDS")...)
				}
				rec = append(rec, string(val))
			}
		} else {
			val = []byte("-")
			rec = append(rec, string(val))
		}

		//fmt.Println(rec)
		err = csvWriter.Write(rec)
		if err != nil {
			panic(err)
		}
	}
	csvWriter.Flush()
	err = f.Close()
	if err != nil {
		panic(err)
	}
}

func main() {
	//GenerateTest("tests/w0002.csv", 1000, 20, 18, 15)
	//fmt.Println("DONE GENERATING")
	conf, err := coreconf.LoadConfig("conf.yaml")
	if err != nil {
		panic(err)
	}
	wen := wrappereng.New(conf)
	Test(wen, "tests/w0001.csv")
}
