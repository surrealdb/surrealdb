/**
[test]

[test.results]
parsing-error = """
Unexpected token `a number` expected delimiter `}`
  --> [18:10]
   |
18 | RETURN {,1};
   |          ^
  --> [18:8]
   |
18 | RETURN {,1};
   |        ^ expected this delimiter to close
"""
*/

RETURN {,1};
