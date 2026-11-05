/**
[env]
imports = ["reproductions/alter_field_inline_checks_the_definition_import.surql"]

[test]
reason = """
ALTER FIELD ... INLINE checks the definition it forms as DEFINE FIELD ... INLINE
checks it: a COMPUTED field is refused, since its value is derived at read time
and nothing is stored to embed, and so is a field that would be a table's 256th
INLINE field, since the adjacency payload counts its values in one byte. The
refused statement leaves the field as it was.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "An error occurred: a COMPUTED field cannot be INLINE: its value is derived at read time, so there is nothing stored to embed"

[[test.results]]
value = "'DEFINE FIELD c ON e TYPE none | int COMPUTED w + 1 PERMISSIONS FULL'"

[[test.results]]
error = "An error occurred: a table cannot carry more than 255 INLINE fields: the adjacency payload's value count is a single byte"

[[test.results]]
value = "'DEFINE FIELD f255 ON wide TYPE none | int PERMISSIONS FULL'"

*/

DEFINE TABLE e TYPE RELATION SCHEMAFULL;
DEFINE FIELD w ON e TYPE option<int>;
DEFINE FIELD c ON e TYPE option<int> COMPUTED w + 1;
ALTER FIELD c ON e INLINE;
(INFO FOR TABLE e).fields.c;
ALTER FIELD f255 ON wide INLINE;
(INFO FOR TABLE wide).fields.f255;
