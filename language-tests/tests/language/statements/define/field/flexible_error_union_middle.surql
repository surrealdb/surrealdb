/**
[test]
reason = "FLEXIBLE cannot appear inside a union type"

[test.results]
parsing-error = """
Unexpected token `|`, expected Eof
  --> [17:55]
   |
17 | ... FLEXIBLE | string;
   |              ^
"""

*/

DEFINE TABLE test SCHEMAFULL;
DEFINE FIELD wrong ON test TYPE int | object FLEXIBLE | string;
