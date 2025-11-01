/**
[test]

[test.results]
parsing-error = """
`FLEXIBLE` is no longer supported, use `TYPE any` or `TYPE object` instead
  --> [17:21]
   |
17 | DEFINE FIELD a ON t FLEXIBLE TYPE number;
   |                     ^^^^^^^^
"""


*/

DEFINE TABLE t;
DEFINE FIELD a ON t FLEXIBLE TYPE number;
