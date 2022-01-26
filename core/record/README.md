```
record
    - Data structure in the form of (key, val) pairs with additional info
    - UNIX timestamp for creation
    - Uses Crc32 as checksum (only key and value are checked)
    - Supports serialization/deserialization
    - Format:

        +-------+-----------+--------+---------+---------+---------+-   ...   -+-    ...    -+
        |  CRC  | Timestamp | Status | TypeInfo| KeySize | ValSize |    Key    |     Val     |
        +-------+-----------+--------+---------+---------+---------+-   ...   -+-    ...    -+

        32bit   64bit       8bit     8bit      64bit     64bit     Variable    Variable

        KeySize and ValSize are measured in bytes, for the corresponding arrays.

    - Status field holds record metadata. For now, only the tombstone is used.
    - TypeInfo field holds type information used by application layers that wrap around nakevaleng.
      When a Record is created by nakevaleng, its TypeInfo field is 0 which represents "no type" or
      "any type" (akin to void* in C). The engine itself does not manipulate this field, except when
      calling New*() and Clone().  

       

```

```go

// Create new

rec1 := record.NewFromString("Key01", "Val01")
rec2 := record.NewFromString("Key02", "Val02")

// Change type

rec1.TypeInfo = 5 // Meaningless without context

// Clone

rec1_clone := record.Clone(rec1)

// Print

fmt.Println("Rec1:", rec1.ToString())
fmt.Println("Rec2:", rec2.ToString())
fmt.Println("Rec1 Clone:", rec1_clone.ToString())

// Check its tombstone

fmt.Println("Is it deleted:", rec1.IsDeleted()) // Should be false

// Append to file

os.Remove("record.bin")

rec1.Serialize("record.bin")
rec2.Serialize("record.bin")

// Read from file

rec1_from_file := record.NewEmpty()
rec2_from_file := record.NewEmpty()

{
    f, _ := os.OpenFile("data/record.bin", os.O_APPEND, 0666)
    defer f.Close()
    w := bufio.NewWriter(f)
    defer w.Flush()

    rec1.Serialize(w)
    rec2.Serialize(w)
}

fmt.Println("Rec1:", rec1_from_file.ToString())
fmt.Println("Rec2:", rec2_from_file.ToString())

```