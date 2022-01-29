```
datatable.go
    functions for writing the key-value record objects from a memtable onto disk
    given the data and filename- and lsm tree-related data, we can construct the data table
    a data table has the following format

        | R | R | R | R | R | R | R | ...

    where | R | is a single record as defined in core/record

indextable.go
    defines a basic internal interface for reading/writing an index table, which is used for faster
    lookups during random access reads
    an index table is comprised of index table entries (ITEs) which have the following format (as
    defined by the indexTableEntry struct):

        +---------+----------+------ ... ------+
        | KeySize |  Offset  |       Key       |
        +---------+----------+------ ... ------+
         8 byte    8 byte     variable size

        KeySize
            - length of the Key field, in bytes
            - this *must* have the same value as the key size of the record this ITE is pointing to
        Offset
            - relative address of the record with the matching key inside the data table, in bytes
            - in other words, for a record with key K that's N bytes away from the start of the data
            table's beginning, the ITE with the same key K will have N as its offset
        Key
            - the actual key, in bytes
            - this *must* have the same value as the key of the record this ITE is pointing to

summarytable.go
    a summary is a sparse index used to speed up lookups in the corresponding index table
    each summary table entry (STE) points to one ITE, such that every K ITEs are assigned one STE
    this way, every K-th ITE has a "copy" in the STE
    the total number of searches is N/K + K instead of N (plot to see when this is better)
    a summary table is comprised of STEs which have the following format (as defined by the 
    summaryTableEntry struct):

        +---------+----------+------ ... ------+
        | KeySize |  Offset  |       Key       |
        +---------+----------+------ ... ------+
         8 byte    8 byte     variable size

        KeySize
            - length of the Key field, in bytes
            - this *must* have the same value as the key size of the index this STE is pointing to
        Offset
            - relative address of the ITE with the matching key inside the data table, in bytes
        Key
            - the actual key, in bytes
            - this *must* have the same value as the key of the ITE this STE is pointing to 

    propagation of minimal key is used, therefore:
        for some ITE between STE1's pointing ITE and STE2's pointing ITE, the following holds:

                                STE1.Key <= ITE.Key <= STE2.Key
                        (it can't happen that both inequalities are <=)

sstable.go
    in a narrow sense, only the data table is an SSTable, other tables are used to speed up lookup
    in a broader sense, all the tables together + everything else froms a single SSTable
    "everything else" includes:
        - filter    (bloom filter)
        - metadata  (merkle tree)
        - ...
```

