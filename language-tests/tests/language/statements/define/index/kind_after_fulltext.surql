/**
[test]
reason = "An index has one kind, so a UNIQUE clause after FULLTEXT is a parse error rather than a replacement of the full-text index"
issue = 1495

[test.results]
parsing-error = """
Cannot combine UNIQUE with FULLTEXT
  --> [21:82]
   |
21 | ...IGHLIGHTS UNIQUE;
   |              ^^^^^^ an index can only be one of UNIQUE, COUNT, FULLTEXT, HNSW, DISKANN or SPATIAL
  --> [21:41]
   |
21 | DEFINE INDEX title ON book FIELDS title FULLTEXT ANALYZER simple BM25 HIGHLIG...
   |                                         ^^^^^^^^ First given here
"""
*/

DEFINE ANALYZER simple TOKENIZERS blank;
DEFINE INDEX title ON book FIELDS title FULLTEXT ANALYZER simple BM25 HIGHLIGHTS UNIQUE;
