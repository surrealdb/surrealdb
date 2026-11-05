/**
[test]
reason = "SQL must be followed by TYPE: SQL POSTGRESQL is a parse error"

[test.results]
parsing-error = """
Unexpected token `an identifier`, expected TYPE
  --> [16:34]
   |
16 | DEFINE FIELD f ON t TYPE int SQL POSTGRESQL 'int4';
   |                                  ^^^^^^^^^^
"""
*/

DEFINE TABLE t;
DEFINE FIELD f ON t TYPE int SQL POSTGRESQL 'int4';
