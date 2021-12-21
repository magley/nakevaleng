```
bloomfilter
	- bloom filter
	- compact data (all bits are used)
	- data is hashed using murmur3
	- supports serialization (using gob encoding)
```

```go

// Create bloom filter.

bf := New(10, 0.2)

// Insert elements.

bf.Insert([]byte("KEY00"))
bf.Insert([]byte("KEY01"))
bf.Insert([]byte("KEY02"))
bf.Insert([]byte("KEY03"))
bf.Insert([]byte("KEY05"))

// Query elements (true, false).

fmt.Println(bf.Query([]byte("KEY00")))
fmt.Println(bf.Query([]byte("KEY04")))

// Insert and query again (true).

bf.Insert([]byte("KEY04"))
fmt.Println(bf.Query([]byte("KEY04")))

// Serialize & deserialize (true)

bf.EncodeToFile("filter.db")

bf2 := DecodeFromFile("filter.db")
fmt.Println(bf2.Query([]byte("KEY04")))

```