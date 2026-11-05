/**
[env]
imports = ["upgrade/define/3.00.00_field_overwrite_import.surql"]

[test]
upgrade = true
reason = """
Fields a release before 3.3 wrote store their TYPE as a `Kind`, a revision the
current checkout converts on read. `DEFINE FIELD … OVERWRITE` reads the stored
definition it replaces, so it must accept that revision: each overwritten field
answers with its new clauses, the field left alone keeps its converted type,
and writes afterwards are checked against the new definitions."""
# OVERWRITE over these revisions and the current INFO rendering are the current checkout's.
version = ">=3.4.0-a"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ age: 'DEFINE FIELD age ON person TYPE int | string PERMISSIONS FULL', name: "DEFINE FIELD name ON person TYPE string ASSERT string::len($value) > 0 COMMENT 'display name' PERMISSIONS FULL", nick: "DEFINE FIELD nick ON person TYPE none | string DEFAULT 'none' PERMISSIONS FULL" }"""

[[test.results]]
value = "[{ age: 'unknown', id: person:2, name: 'b', nick: 'none' }]"

[[test.results]]
error = "Found '' for field `name`, with record `person:3`, but field must conform to: string::len($value) > 0"

[[test.results]]
value = "[{ age: 3, id: person:1, name: 'a', nick: 'none' }, { age: 'unknown', id: person:2, name: 'b', nick: 'none' }]"

*/
-- Widen the type of a field written by the old binary.
DEFINE FIELD OVERWRITE age ON person TYPE int | string;
-- Replace a field's assertion and add a comment.
DEFINE FIELD OVERWRITE name ON person TYPE string ASSERT string::len($value) > 0 COMMENT 'display name';
-- The overwritten fields carry their new clauses; `nick` is as the old binary wrote it.
RETURN (INFO FOR TABLE person).fields;
-- A write the old `age` refused is accepted under the new type.
CREATE person:2 SET name = 'b', age = 'unknown';
-- The new assertion on `name` is enforced.
CREATE person:3 SET name = '', age = 1;
-- The record the old binary wrote is read unchanged beside the new one.
SELECT * FROM person ORDER BY id;
