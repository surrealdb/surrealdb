/**
[env]
namespace = true
database = true

[test]
reason = """
A `SPLIT` target must name a field of the row the selection returns, under the name the
field is returned as. An alias returns its field under the alias, so the source name of an
aliased field is refused rather than splitting nothing.
"""

[test.results]
parsing-error = """
Missing split idiom `"tags"` in statement selection
  --> [28:38]
   |
28 | SELECT tags AS t FROM person:a SPLIT tags;
   |                                      ^^^^
  --> [28:8]
   |
28 | SELECT tags AS t FROM person:a SPLIT tags;
   |        ^^^^^^^^^ Idiom missing here
"""

*/

SELECT tags AS t FROM person:a SPLIT tags;
