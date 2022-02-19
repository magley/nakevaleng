```
skiplistnode
    - single node of a Skiplist
    - stores data as a Record object (see core/record)
    - pointers to adjacent nodes are stored level wise, for example:

    LVL 3     |    |                              |    |
    LVL 2     |    |                  |    |      |    |   
    LVL 1     |    |      |    |      |    |      |    |    
    LVL 0     | N1 |      | N2 |      | N3 |      | N4 |

    N1.Next = [ &N2, &N2, &N3, &N4 ]
    N2.Next = [ &N3, &N3 ]
    N3.Next = [ &N4, &N4, &N4 ]
    N4.Next = [ nil, nil, nil, nil ]

skiplist
    - the Skiplist
    - it has a maximum level and the current level
    - removing nodes is done by marking the record's tombstone
    - inserting and updating are one function - i.e. we use an "upsert" command
    - searching for a node can optionally ignore removed nodes
    - the skiplist is structured using a header which points to other nodes, for example

    LVL n-1       |        |                                     < nil >
      ...         |        |                                     < nil >
     LVL 2        |        |    |    |                           < nil >
     LVL 1        |        |    |    |                 |    |    < nil >
     LVL 0        | HEADER |    | N1 |    | N2 |  ...  | Nk |    < nil >

    - n is the maximum level in the list
    - k is the number of nodes
    - assumming that N1 is the "tallest" node, the level of the skiplist is then 2
    - the header is always present in the skiplist, always has n levels and never stores data
    - only the HEADER is stored inside the skiplist, everything else is dynamic (see skiplistnode)
    - the < nil > column is not a part of the skiplist, it's drawn here for a better understanding
```

```go

// Create new 

skiplist, _ := skiplist.New(3)

{
    // Some data

    r1 := record.New([]byte("Key01"), []byte("Val01"))
    r2 := record.New([]byte("Key02"), []byte("Val05"))
    r3 := record.New([]byte("Key03"), []byte("Val02"))
    r4 := record.New([]byte("Key04"), []byte("Val04"))

    r1.TypeInfo = 1 // e.g. TypeInfo 1 == CountMinSketch
    r2.TypeInfo = 2 // e.g. TypeInfo 2 == HyperLogLog

    // Insert into skiplist

    skiplist.Write(r1)
    skiplist.Write(r3)
    skiplist.Write(r4)
    skiplist.Write(r2)
}

// Key-based find

fmt.Println("Find Key01...", skiplist.Find([]byte("Key01"), true).Data.ToString())
fmt.Println("Find Key02...", skiplist.Find([]byte("Key02"), true).Data.ToString())

// Change with new type

{
    r4_new := skiplist.Find([]byte("Key04"), true).Data
    r4_new.TypeInfo = 3
    skiplist.Write(r4_new)
}

fmt.Println("Find Key04...", skiplist.Find([]byte("Key04"), true).Data.ToString())

// Remove elements

skiplist.Remove([]byte("Key05"))
skiplist.Remove([]byte("Key07")) // Shouldn't do anything since Key07 was not in our skiplist.
fmt.Println("Find Key05 (removed)...", skiplist.Find([]byte("Key05"), true))
fmt.Println("Find Key07 (noexist)...", skiplist.Find([]byte("Key05"), true))

// Iterate through all nodes

fmt.Println("All the nodes:")
{
    n := skiplist.Header.Next[0]
    for n != nil {
        fmt.Println(n.Data.ToString())
        n = n.Next[0]
    }
}

// Clear the list

skiplist.Clear()
fmt.Println("All the nodes after clearing the list:")
{
    n := skiplist.Header.Next[0]
    for n != nil {
        fmt.Println(n.Data.ToString())
        n = n.Next[0]
    }
}

```