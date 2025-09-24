/**
[test]

[test.results]
parsing-error = """
Expected one column for fulltext index, found 2
  --> [14:30]
   |
14 | DEFINE INDEX i ON b FIELDS a,b FULLTEXT BM25 HIGHLIGHTS;
   |                              ^ 
"""

*/
DEFINE INDEX i ON b FIELDS a,b FULLTEXT BM25 HIGHLIGHTS;
