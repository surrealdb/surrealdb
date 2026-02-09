/**
[test]

[test.results]
parsing-error = """
Unexpected token `;`, expected `COMPACT`, `DROP` or `QUERY_TIMEOUT`
  --> [14:13]
   |
14 | ALTER SYSTEM;
   |             ^
"""

*/
ALTER SYSTEM;