/**
[test]
reason = "An index kind clause may appear once, so UNIQUE given twice is a parse error"
issue = 1495

[test.results]
parsing-error = """
UNIQUE is given twice
  --> [20:48]
   |
20 | DEFINE INDEX email ON user FIELDS email UNIQUE UNIQUE;
   |                                                ^^^^^^ an index can only be one of UNIQUE, COUNT, FULLTEXT, HNSW, DISKANN or SPATIAL
  --> [20:41]
   |
20 | DEFINE INDEX email ON user FIELDS email UNIQUE UNIQUE;
   |                                         ^^^^^^ First given here
"""
*/

DEFINE INDEX email ON user FIELDS email UNIQUE UNIQUE;
