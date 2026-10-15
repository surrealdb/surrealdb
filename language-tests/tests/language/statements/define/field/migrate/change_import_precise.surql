/**
[test]
wip = true
reason = "Decision open: the first OPTION IMPORT write demotes every Guaranteed field of the table, not only the misfit one, and no row is flagged UNCHECKED. Change 33: an OPTION IMPORT row demotes only the field it misfits. A row that fits every field leaves every Guaranteed field Guaranteed, and a row that misfits one field leaves the others Guaranteed, on a table that is enforced or is not."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['a', 'b']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['a', 'b']"

[[test.results]]
value = "[{ a: 1, b: 2, id: fit:1 }, { a: 5, b: 6, id: fit:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['b']"

[[test.results]]
value = "[{ a: 1, b: 2, id: misfit:1 }, { a: 'x', b: 7, id: misfit:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'd3', namespace: 'test' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['a']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['b']"
*/
-- Enforced table: every field is Guaranteed by a migration.
DEFINE TABLE fit SCHEMAFULL;
DEFINE FIELD a ON fit TYPE string;
DEFINE FIELD b ON fit TYPE string;
CREATE fit:1 SET a = '1', b = '2' RETURN NONE;
ALTER FIELD a ON fit TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
ALTER FIELD b ON fit TYPE int MIGRATE EXISTING ROWS AS b_int USING <int>$value FALLBACK 0;
(INFO FOR TABLE fit).migrations.guaranteed;
-- A row that fits both.
OPTION IMPORT;
CREATE fit:2 SET a = 5, b = 6 RETURN NONE;
OPTION IMPORT = false;
(INFO FOR TABLE fit).migrations.guaranteed;
SELECT id, a, b FROM fit ORDER BY id;
-- A row that misfits `a` only.
DEFINE TABLE misfit SCHEMAFULL;
DEFINE FIELD a ON misfit TYPE string;
DEFINE FIELD b ON misfit TYPE string;
CREATE misfit:1 SET a = '1', b = '2' RETURN NONE;
ALTER FIELD a ON misfit TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
ALTER FIELD b ON misfit TYPE int MIGRATE EXISTING ROWS AS b_int USING <int>$value FALLBACK 0;
OPTION IMPORT;
CREATE misfit:2 SET a = 'x', b = 7 RETURN NONE;
OPTION IMPORT = false;
(INFO FOR TABLE misfit).migrations.guaranteed;
SELECT id, a, b FROM misfit ORDER BY id;
-- Not enforced.
DEFINE DATABASE d3 COMPATIBILITY 3;
USE DB d3;
DEFINE TABLE fit SCHEMAFULL;
DEFINE FIELD a ON fit TYPE string;
CREATE fit:1 SET a = '1' RETURN NONE;
ALTER FIELD a ON fit TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
OPTION IMPORT;
CREATE fit:2 SET a = 5 RETURN NONE;
OPTION IMPORT = false;
(INFO FOR TABLE fit).migrations.guaranteed;
DEFINE TABLE misfit SCHEMAFULL;
DEFINE FIELD a ON misfit TYPE string;
DEFINE FIELD b ON misfit TYPE string;
CREATE misfit:1 SET a = '1', b = '2' RETURN NONE;
ALTER FIELD a ON misfit TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
ALTER FIELD b ON misfit TYPE int MIGRATE EXISTING ROWS AS b_int USING <int>$value FALLBACK 0;
OPTION IMPORT;
CREATE misfit:2 SET a = 'x', b = 7 RETURN NONE;
OPTION IMPORT = false;
(INFO FOR TABLE misfit).migrations.guaranteed;
