package sstable

import (
	"nakevaleng/core/skiplist"
	"nakevaleng/ds/bloomfilter"
	"nakevaleng/ds/merkletree"
	"nakevaleng/util/filename"
)

func MakeTable(path string, dbname string, level int, run int, list *skiplist.Skiplist) {
	makeDataTable(path, dbname, level, run, list)
	makeIndexTable(path, dbname, level, run, list)
	makeSummaryTable(path, dbname, level, run, 2)
	makeFilter(path, dbname, level, run, list)
	makeMetadata(path, dbname, level, run, list)
}

func makeFilter(path string, dbname string, level int, run int, list *skiplist.Skiplist) {
	elemNo := 0 // TODO: Keep the count inside the skiplist? It doesn't affect performance anyway.
	{
		n := list.Header.Next[0]
		for n != nil {
			elemNo += 1
			n = n.Next[0]
		}
	}

	bf := bloomfilter.New(elemNo, 0.01)
	{
		n := list.Header.Next[0]
		for n != nil {
			bf.Insert(n.Data.Key)
			n = n.Next[0]
		}
	}

	bf.EncodeToFile(filename.Table(path, dbname, level, run, filename.TypeFilter))
}

func makeMetadata(path string, dbname string, level int, run int, list *skiplist.Skiplist) {
	merkleNodes := make([]merkletree.MerkleNode, 0)
	{
		n := list.Header.Next[0]
		for n != nil {
			merkleNodes = append(merkleNodes, merkletree.MerkleNode{Data: n.Data.Value})
			n = n.Next[0]
		}
	}

	merkleTree := merkletree.New(merkleNodes)
	merkleTree.Serialize(filename.Table(path, dbname, level, run, filename.TypeMetadata))
}
