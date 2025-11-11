/**
[test]

[test.results]
parsing-error = """
Expected one column, found none
  --> [14:52]
   |
14 | ... EUCLIDEAN;
   |              ^
"""

*/
DEFINE INDEX i ON t HNSW DIMENSION 4 DIST EUCLIDEAN;
