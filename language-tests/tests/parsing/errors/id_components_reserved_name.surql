/**
[test]
reason = "A record id component cannot be called id, in or out: every record already has those fields."

[test.results]
parsing-error = """
the record id component name `in` is reserved
  --> [15:48]
   |
15 | DEFINE FIELD id ON reading TYPE [city: string, in: datetime];
   |                                                ^^
"""

*/
DEFINE FIELD id ON reading TYPE [city: string, in: datetime];
