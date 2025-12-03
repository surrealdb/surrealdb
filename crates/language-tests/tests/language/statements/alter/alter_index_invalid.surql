/**
[test]

[test.results]
parsing-error = """
Unexpected token `;`, expected `PREPARE`, `DROP` or `COMMENT`
  --> [16:19]
   |
16 | ALTER INDEX a ON t;
   |                   ^
"""


*/

ALTER INDEX a ON t;