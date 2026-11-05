/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/
-- One keyword field per table, and the table defined explicitly first. A
-- version that cannot re-read a stored keyword field name also fails the schema
-- re-validation that runs before every later DEFINE FIELD on the same table and
-- before every write to it, so a second field on the same table, or a CREATE
-- here, would make the import itself fail instead of writing the schema this
-- test needs.
DEFINE TABLE kwfn SCHEMAFULL;
DEFINE FIELD `function` ON kwfn TYPE string;
DEFINE TABLE kwsel SCHEMAFULL;
DEFINE FIELD `select` ON kwsel TYPE string;
-- An array field also writes the definition its kind implies for the
-- elements, `select.*` and `value.*`, which the releases in the matrix key by
-- the escaped rendering of the path.
DEFINE TABLE kwarr SCHEMAFULL;
DEFINE FIELD `select` ON kwarr TYPE array<string>;
DEFINE TABLE kwrm SCHEMAFULL;
DEFINE FIELD `value` ON kwrm TYPE array<string>;
