/**
[test]
reason = "A component name is an identifier, not a string."

[test.results]
parsing-error = """
Unexpected token `:`, expected a kind name
  --> [15:40]
   |
15 | DEFINE FIELD id ON reading TYPE ['city': string];
   |                                        ^
"""

*/
DEFINE FIELD id ON reading TYPE ['city': string];
