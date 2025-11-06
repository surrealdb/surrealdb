/**
[test]

[test.results]
parsing-error = """
Cannot create a count index with fields
  --> [14:28]
   |
14 | DEFINE INDEX a ON b FIELDS c COUNT;
   |                            ^
"""

*/
DEFINE INDEX a ON b FIELDS c COUNT;
