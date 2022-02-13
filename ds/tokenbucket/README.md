```
tokenbucket
```

```go

maxTokens := 2
resetInterval := 4 // seconds

tb := tokenbucket.New(maxTokens, resetInterval)

for {
    tb.HasEnoughTokens()
    time.Sleep(1 * time.Second)
}

```