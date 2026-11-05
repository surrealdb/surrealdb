/**
[env]
namespace = true
database = true

[test]
reason = """
A `SELECT VALUE` returns its expression as the value of the row, so a `SPLIT` target must name
that value. The expression under an alias is named by the alias, so the source name of an aliased
value is refused.
"""

[test.results]
parsing-error = """
Missing split idiom `"tags"` in statement selection
  --> [28:44]
   |
28 | SELECT VALUE tags AS t FROM person:a SPLIT tags;
   |                                            ^^^^
  --> [28:8]
   |
28 | SELECT VALUE tags AS t FROM person:a SPLIT tags;
   |        ^^^^^^^^^^^^^^^ Idiom missing here
"""

*/

SELECT VALUE tags AS t FROM person:a SPLIT tags;
