/**
[test]

[test.results]
parsing-error = """
Invalid token, found unexpected character `u` after number token
  --> [14:1]
   |
14 | 1.5u8;
   | ^^^^
"""
*/

1.5u8;
