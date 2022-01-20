```
record
    - Data structure in the form of (key, val) pairs with additional info
    - UNIX timestamp for creation
    - Uses Crc32 as checksum (only key and value are checked)
    - Supports serialization/deserialization
    - Format:

        +-------+-----------+--------+---------+---------+-   ...   -+-    ...    -+
        |  CRC  | Timestamp | Status | KeySize | ValSize |    Key    |     Val     |
        +-------+-----------+--------+---------+---------+-   ...   -+-    ...    -+

        32bit   64bit       8bit     64bit     64bit     Variable    Variable

        KeySize and ValSize are measured in bytes, for the corresponding arrays.

    - Format of Status field:

        8                   3     2     1     0
        +------- ... -------+-----+-----+-----+
        |                   | CMS | HLL |  T  |
        +------- ... -------+-----+-----+-----+

        ^ ^    Reserved   ^ ^

        T
            Tombstone
            1 if the record is flagged as "deleted"
            0 otherwise
        HLL
            Does this record's Value field represent the bytes for a HyperLogLog?
            1 if yes
            0 if no
        CMS
            Does this record's Value field represent the bytes for a CountMinSketch?
            1 if yes
            0 if no

        It's UB if HLL and CMS are both set to 1. 

```

```go

rec1 := record.NewFromString("Key01", "Val01")
rec2 := record.NewFromString("Key02", "Val02")

// Print

fmt.Println("Rec1:", rec1.ToString())
fmt.Println("Rec2:", rec2.ToString())

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
    f, _ := os.OpenFile("record.bin", os.O_RDONLY, 0666)
    defer f.Close()
    w := bufio.NewReader(f)

    rec1_from_file.Deserialize(w) // Should equal rec1
    rec2_from_file.Deserialize(w) // Should equal rec2
}

fmt.Println("Rec1:", rec1_from_file.ToString())
fmt.Println("Rec2:", rec2_from_file.ToString())

```