/**
[test]
reason = "SQL TYPE names its dialect as POSTGRESQL, MYSQL or DYNAMODB: any other word is a parse error"

[test.results]
parsing-error = """
Unexpected token `an identifier`, expected `POSTGRESQL`, `MYSQL`, or `DYNAMODB`
  --> [16:39]
   |
16 | DEFINE FIELD f ON t TYPE int SQL TYPE ORACLE 'number';
   |                                       ^^^^^^
"""
*/

DEFINE TABLE t;
DEFINE FIELD f ON t TYPE int SQL TYPE ORACLE 'number';
