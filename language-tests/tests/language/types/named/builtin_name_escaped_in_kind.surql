/**
[env]
namespace = true
database = true

[test]
reason = "An escaped built-in kind name in a kind position names no type, so it is refused rather than stored as a type reference that reads back as the built-in kind"

[test.results]
parsing-error = """
Unexpected token `an identifier`, expected a kind name
  --> [19:26]
   |
19 | DEFINE FIELD f ON t TYPE `int`;
   |                          ^^^^^
"""
*/

DEFINE FIELD f ON t TYPE `int`;
