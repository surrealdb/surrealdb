/**
[test]

[test.results]
parsing-error = """
Expected at least one column - Use FIELDS to define columns
  --> [14:27]
   |
14 | DEFINE INDEX a ON b UNIQUE;
   |                           ^
"""

*/
DEFINE INDEX a ON b UNIQUE;
