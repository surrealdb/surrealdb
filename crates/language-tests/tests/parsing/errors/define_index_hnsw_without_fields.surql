/**
[test]

[test.results]
parsing-error = """
Expected one column, found none
"""

*/
DEFINE INDEX i ON t HNSW DIMENSION 4 DIST EUCLIDEAN;
