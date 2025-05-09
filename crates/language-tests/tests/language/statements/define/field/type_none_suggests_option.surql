/**
[test]

[test.results]
parsing-error = """
Unexpected token `NONE`, expected a kind name.
  --> [15:41]
   |
15 | DEFINE FIELD name ON user TYPE string | NONE;
   |                                         ^^^^ to define a field that can be NONE, use option<type_name> instead.
"""

*/

DEFINE FIELD name ON user TYPE string | NONE;