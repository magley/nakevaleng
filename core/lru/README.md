```
lru
    - least recently used cache
```

```go

capacity := 3
lru := lru.New(capacity)

rec1 := record.NewFromString("Key01", "Val01")
rec2 := record.NewFromString("Key02", "Val02")
rec3 := record.NewFromString("Key03", "Val03")
rec4 := record.NewFromString("Key04", "Val04")

lru.Set(rec1)
lru.Set(rec2)
lru.Set(rec3)

// Current order in the cache:
// rec1 rec2 rec3

res, exists := lru.Get("Key01") // Will also bump rec1 to the front of the list

// New order:
// rec2 rec3 rec1

res, exists = lru.Get("Key42") // exists is now false. Has no effect on order.

lru.Set(rec4)

// New order:
// rec3 rec1 rec4

```