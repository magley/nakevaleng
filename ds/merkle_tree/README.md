```
merkle_node
	- merkle tree node which stores a byte sequence
	- supports serialization, with the following format:

		+-------+---------------+
		| FLAGS |     DATA      |
		+-------+---------------+
		
		* FLAGS
			1 byte
			check MerkleNodeFlags in merkle_node.h for a list of flags.

		* DATA
			variable size, but in the case of SHA1, it'll be 40 bytes
			if 'empty node' bit is set, no data is written


merkle_tree
	- merkle tree with bottom-up building, hash evaluation and verification
	- a tree can be built from any number of elements or nodes
	- if the tree is not complete, empty nodes are inserted in-place
	- hashing is done using SHA1
	- supports serialization, with the following format:

		+----+----+----+----+----+----+----+----+-
		| N1 | N2 | N3 | N4 | N5 | N6 | N7 | N8 | ...
		+----+----+----+----+----+----+----+----+-

		where Ni is the i-th merkle node, serialized

		the tree above would then look like:

                     N1
                   /    \
                 /        \
               N2          N3
              /  \        /  \
            N4    N5    N6    N7
           /
         N8
```