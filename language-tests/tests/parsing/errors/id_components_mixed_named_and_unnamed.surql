/**
[test]
reason = "Every element of a named tuple has a name."

[test.results]
parsing-error = """
Unexpected token `DATETIME`, expected a component name
  --> [15:48]
   |
15 | DEFINE FIELD id ON reading TYPE [city: string, datetime];
   |                                                ^^^^^^^^
"""

*/
DEFINE FIELD id ON reading TYPE [city: string, datetime];
