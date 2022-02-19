```
cmsketch
	- count min sketch
	- data is hashed using murmur3
	- supports serialization (using gob encoding)
```

```go

// Create new count-min sketch

cms, _ := New(0.1, 0.1)

// Insert

cms.Insert([]byte("blue"))
cms.Insert([]byte("blue"))
cms.Insert([]byte("red"))
cms.Insert([]byte("green"))
cms.Insert([]byte("blue"))

// Query

fmt.Println("Querying a CMS built in memory, should be: 3, 1, 1, 0, 0")
fmt.Println(cms.Query([]byte("blue")))
fmt.Println(cms.Query([]byte("red")))
fmt.Println(cms.Query([]byte("green")))
fmt.Println(cms.Query([]byte("yellow")))
fmt.Println(cms.Query([]byte("orange")))

// Serialize

cms.EncodeToFile("cms.bin")
cms2 := DecodeFromFile("cms.bin")

fmt.Println("Querying a CMS built from disk, should be: 3, 1, 1, 0, 0")
fmt.Println(cms2.Query([]byte("blue")))
fmt.Println(cms2.Query([]byte("red")))
fmt.Println(cms2.Query([]byte("green")))
fmt.Println(cms2.Query([]byte("yellow")))
fmt.Println(cms2.Query([]byte("orange")))

```