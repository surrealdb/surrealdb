/**
[test]
reason = "An escaped name is the same name as its unescaped spelling."

[test.results]
parsing-error = """
the record id component `city` is named twice
  --> [15:48]
   |
15 | DEFINE FIELD id ON reading TYPE [city: string, `city`: datetime];
   |                                                ^^^^^^
"""

*/
DEFINE FIELD id ON reading TYPE [city: string, `city`: datetime];
