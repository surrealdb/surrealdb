/**
[test]

[test.results]
parsing-error = """
Unexpected token `;`, expected PREPARE
  --> [16:19]
   |
16 | ALTER INDEX a ON t;
   |                   ^
"""


*/

ALTER INDEX a ON t;