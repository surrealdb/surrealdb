/**
[test]
reason = "The name of a built-in kind cannot be defined as a type when it is escaped"

[test.results]
parsing-error = """
The type name 'int' is a built-in type and cannot be defined
  --> [15:1]
   |
15 | DEFINE TYPE `int` AS string;
   | ^^^^^^^^^^^^^^^^^^^^^^^^^^^
"""
*/

DEFINE TYPE `int` AS string;
