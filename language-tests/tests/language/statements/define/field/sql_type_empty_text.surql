/**
[test]
reason = "An empty SQL TYPE text names no type and is refused"

[test.results]
parsing-error = """
A SQL TYPE clause needs a type
  --> [16:50]
   |
16 | DEFINE FIELD f ON t TYPE int SQL TYPE POSTGRESQL '';
   |                                                  ^^
"""
*/

DEFINE TABLE t;
DEFINE FIELD f ON t TYPE int SQL TYPE POSTGRESQL '';
