package main

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"nakevaleng/core/record"
	"nakevaleng/engine/wrappereng"
	"os"
)

func Test(wen wrappereng.WrapperEngine, testPath string) {
	f, err := os.OpenFile(testPath, os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	csvReader := csv.NewReader(f)
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
			wen.Put(rec[0], rec[2], []byte(rec[3]))
		} else if rec[1] == "D" {
			wen.Delete(rec[0], rec[2])
		} else if rec[1] == "G" {
			got, found = wen.Get(rec[0], rec[2])
			if found {
				fmt.Println(rec[2], "returns:", got)
			} else {
				fmt.Println(rec[2], "not found")
			}
		}
	}
}

func GenerateTest(testPath string, commands int, maxLen int) {
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
		if command == "P" {
			val = make([]byte, rand.Intn(maxLen)+1)
			for j := 0; j < len(val); j++ {
				val[j] = byte(65 + rand.Intn(26))
			}
		} else {
			val = []byte("-")
		}

		rec = append(rec, string(key))
		rec = append(rec, string(val))

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
	GenerateTest("tests/w0001.csv", 1000, 20)
	fmt.Println("DONE GENERATING")
	//wen := wrappereng.New(coreconf.LoadConfig("conf.yaml"))
	//Test(wen, "tests/w0001.csv")
}
