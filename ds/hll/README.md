```
hyperloglog
    - data is hashed using murmur3
    - supports serialization using gob encoding
```

```go

precision := 4
hll := hll.New(precision)

hll.Insert([]byte("data1"))
hll.Insert([]byte("data2"))
hll.Insert([]byte("data3"))
hll.Insert([]byte("data4"))

hll.Estimate() // will return 4

// Serialization and deserialization
hll.EncodeToFile("hll.db")
hll2 := DecodeFromFile("hll.db")

```