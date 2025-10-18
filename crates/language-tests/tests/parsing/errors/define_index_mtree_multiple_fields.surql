/**
[test]

[test.results]
parsing-error = """
Expected one column, found 2
  --> [14:30]
   |
14 | DEFINE INDEX i ON t FIELDS a,b MTREE DIMENSION 1;
   |                              ^ 
"""

*/
DEFINE INDEX i ON t FIELDS a,b MTREE DIMENSION 1;
