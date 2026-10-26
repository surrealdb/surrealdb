/**
[test]
reason = "An index has one kind, so a SPATIAL clause after UNIQUE is a parse error rather than a replacement of the unique index"

[test.results]
parsing-error = """
Cannot combine SPATIAL with UNIQUE
  --> [19:45]
   |
19 | DEFINE INDEX loc ON place FIELDS loc UNIQUE SPATIAL;
   |                                             ^^^^^^^ an index can only be one of UNIQUE, COUNT, FULLTEXT, HNSW, DISKANN or SPATIAL
  --> [19:38]
   |
19 | DEFINE INDEX loc ON place FIELDS loc UNIQUE SPATIAL;
   |                                      ^^^^^^ First given here
"""
*/

DEFINE INDEX loc ON place FIELDS loc UNIQUE SPATIAL;
