/**
[test]
reason = "Only the bare field id may name the components of its tuple, not a path through it."

[test.results]
parsing-error = """
Unexpected token `an identifier`, expected a kind name
  --> [15:36]
   |
15 | DEFINE FIELD id.x ON reading TYPE [city: string];
   |                                    ^^^^
"""

*/
DEFINE FIELD id.x ON reading TYPE [city: string];
