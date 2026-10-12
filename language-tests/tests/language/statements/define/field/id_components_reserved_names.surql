/**
[test]
reason = "A record id component cannot be called `id`, `in` or `out`: every record already has those fields. The error points at the name, and nothing is defined."

[test.results]
parsing-error = """
the record id component name `id` is reserved
  --> [15:48]
   |
15 | DEFINE FIELD id ON reading TYPE [city: string, id: datetime];
   |                                                ^^
"""

*/
DEFINE FIELD id ON reading TYPE [city: string, id: datetime];
