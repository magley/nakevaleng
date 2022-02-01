```
datatable.go

    A Data Table is the main file of an SSTable, responsible for storing actual data.

    Format:
        [ R ] [ R ] [ R ] [ R ] [ R ] [ R ] [ R ] ... [ R ]

    Where [ R ] is a single Record of variable size, as defined in core/record.

indextable.go

    An Index Table is a lookup structure that speeds up random access reads for a Data Table.

    Format:
        [ ITE ] [ ITE ] [ ITE ] ... [ ITE ]
    
    Where [ ITE ] is a single Index Table Entry (ITE). 
    Each Record in a Data Table is assigned a single ITE.

    The format of an ITE is defined by the indexTableEntry structure.

summarytable.go

    A Summary Table is a sparse index used to speed up lookups in the Index Table.
    
    Format:
        [ STH ] [ STE ] [ STE ] ... [ STE ]

    Where [ STH ] is a Summary Table Header (STH) and [ STH ] is a Summary Table Entry (STE).
    A group of ITEs form a block, such that each block is assigned one STE.
    Block size is fixed, except for the last block which may have fewer ITEs.

    An STE's key matches the key of the last ITE in a block.
    Searching a Summary Table is done by comparing the maximal key in each block.
    An STH holds the keys of the first and last ITE in the Index Table, for quick range-checks.
    It also keeps the total amount of bytes needed for all [ STE ]s.
    The format of an STE and STH is defined by summaryTableEntry and indexTableEntry structures.

sstable.go

    Responsible for creating an SSTable, which includes all tables mentioned above + filter and metadata. 
```