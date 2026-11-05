/**
[test]

[test.results]
parsing-error = """
Invalid token, found unexpected character `i` after number token
  --> [14:1]
   |
14 | 1e3i32;
   | ^^^^
"""
*/

1e3i32;
