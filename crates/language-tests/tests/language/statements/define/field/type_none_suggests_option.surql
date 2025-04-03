/**
[test]

[[test.results]]
error = "There was a problem with the database: Parse error: Unexpected token `NONE`, expected a kind name.
Hint: to define a field that can be NONE, use option<type_name> instead.
 --> [1:41]
  |
1 | DEFINE FIELD name ON user TYPE string | NONE;
  |                                         ^^^^ 
"

*/

DEFINE FIELD name ON user TYPE string | NONE;