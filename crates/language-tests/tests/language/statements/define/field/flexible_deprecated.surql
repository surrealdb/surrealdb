/**
[test]

[test.results]
parsing-error = """
FLEXIBLE must be specified after TYPE
  --> [17:21]
   |
17 | DEFINE FIELD a ON t FLEXIBLE TYPE number;
   |                     ^^^^^^^^
"""


*/

DEFINE TABLE t;
DEFINE FIELD a ON t FLEXIBLE TYPE number;
