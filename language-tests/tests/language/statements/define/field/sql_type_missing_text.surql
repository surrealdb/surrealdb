/**
[test]
reason = "SQL TYPE takes its type as a string literal after the dialect: a dialect without text is a parse error"

[test.results]
parsing-error = """
Unexpected token `;`, expected a strand
  --> [16:49]
   |
16 | DEFINE FIELD f ON t TYPE int SQL TYPE POSTGRESQL;
   |                                                 ^
"""
*/

DEFINE TABLE t;
DEFINE FIELD f ON t TYPE int SQL TYPE POSTGRESQL;
