/**
[test]
reason = "A record id component name may be used once. The error points at the second use."

[test.results]
parsing-error = """
the record id component `city` is named twice
  --> [15:48]
   |
15 | DEFINE FIELD id ON reading TYPE [city: string, city: datetime];
   |                                                ^^^^
"""

*/
DEFINE FIELD id ON reading TYPE [city: string, city: datetime];
