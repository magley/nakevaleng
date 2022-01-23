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

// Create a new skiplist

skiplist := skiplist.New(3)

// Insert data using key-value pairs (not recommended)

skiplist.WriteKeyVal([]byte("Key01"), []byte("Val01"))

// Insert data using a pre-existing record (recommended)

rec := Record{...}
skiplist.Write(rec)

// Find by key

nodePtr1 := skiplist.Find([]byte("Key01"), true)  // Will ignore removed nodes
nodePtr2 := skiplist.Find([]byte("Key01"), false) // Will NOT ignore removed nodes

// Remove element by key

skiplist.Remove([]byte("Key01"))

// Update existing element using key-value pairs

skiplist.WriteKeyVal([]byte("Key01"), []byte("Key01 ***UPDATED***"))

// Update existing element using a pre-existing record

rec2 := Record{...}
skiplist.Write(rec2)
skiplist.WriteKeyVal(rec2)

// Iterate through the nodes

n := skiplist.Header.Next[0]
for n != nil {
    fmt.Println(n.Data.ToString())
    n = n.Next[0]
}

// Clear the list

skiplist.Clear()

```