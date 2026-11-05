/**
[test]
reason = "An index has one kind, so a UNIQUE clause after HNSW is a parse error rather than a replacement of the vector index"
issue = 1495

[test.results]
parsing-error = """
Cannot combine UNIQUE with HNSW
  --> [20:53]
   |
20 | ...MENSION 2 UNIQUE;
   |              ^^^^^^ an index can only be one of UNIQUE, COUNT, FULLTEXT, HNSW, DISKANN or SPATIAL
  --> [20:36]
   |
20 | DEFINE INDEX emb ON doc FIELDS emb HNSW DIMENSION 2 UNIQUE;
   |                                    ^^^^ First given here
"""
*/

DEFINE INDEX emb ON doc FIELDS emb HNSW DIMENSION 2 UNIQUE;
