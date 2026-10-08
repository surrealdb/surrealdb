/**
[test]
reason = "A field definition is checked against the definitions the statements before it in its block made, and in a block that defines conditionally"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Computed fields cannot be indexed. Index: 'by_c' - Field: 'c'"

[[test.results]]
error = "Cannot define field `o` as `COMPUTED` since a nested field `o.sub` already exists."

[[test.results]]
error = "Cannot define nested field `p.sub` as parent field `p` is a `COMPUTED` field."

[[test.results]]
error = "Computed fields cannot be indexed. Index: 'by_d' - Field: 'd'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/

DEFINE TABLE t;
{ DEFINE INDEX by_c ON t FIELDS c; DEFINE FIELD c ON t COMPUTED 1; };
{ DEFINE FIELD o.sub ON t TYPE int; DEFINE FIELD o ON t COMPUTED 1; };
{ DEFINE FIELD p ON t COMPUTED 1; DEFINE FIELD p.sub ON t TYPE int; };
{ IF count(SELECT id FROM t) = 0 { DEFINE INDEX by_d ON t FIELDS d }; DEFINE FIELD d ON t COMPUTED 1; };
{ IF count(SELECT id FROM t) > 0 { DEFINE INDEX by_e ON t FIELDS e }; DEFINE FIELD e ON t COMPUTED 1; };
object::keys((INFO FOR TABLE t).indexes);
