/**
[test]
reason = "The name of a built-in kind cannot be defined as a type"

[test.results]
parsing-error = """
The type name 'string' is a built-in type and cannot be defined
  --> [16:1]
   |
16 | DEFINE TYPE string AS int;
   | ^^^^^^^^^^^^^^^^^^^^^^^^^
"""

*/

DEFINE TYPE string AS int;
