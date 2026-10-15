/**
[test]
reason = "Change 33: an OPTION IMPORT write that does not fit a field's type is stored as written on every table, at either compatibility level, with rows or none, and never stops the import; the field it misfits no longer guarantees its type, no generation is minted, rows written before the import read as they did, and writes after it are coerced again."

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
value = "7"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "false"

[[test.results]]
value = "7"

[[test.results]]
value = "[{ a: 1, ai: true, b: 2, bi: true, id: p:1 }, { a: 'x', ai: false, b: 7, bi: true, id: p:2 }, { a: '5', ai: false, b: '6', bi: false, id: p:3 }]"

[[test.results]]
error = "Couldn't coerce value for field `a` of `p:4`: Expected `int` but found `'y'`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 4, ai: true, id: p:4 }]"

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
value = "[{ a: 'x', ai: false, id: e:1 }]"

[[test.results]]
value = "2"

[[test.results]]
error = "Couldn't coerce value for field `a` of `e:2`: Expected `int` but found `'x'`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "2"

[[test.results]]
value = "[{ a: 1, ai: true, id: loose:1 }, { a: 'x', ai: false, id: loose:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'd3', namespace: 'test' }"

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
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "false"

[[test.results]]
value = "7"

[[test.results]]
value = "[{ a: 1, ai: true, b: 2, bi: true, id: p:1 }, { a: 'x', ai: false, b: 7, bi: true, id: p:2 }]"

[[test.results]]
error = "Couldn't coerce value for field `a` of `p:3`: Expected `int` but found `'y'`"

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
value = "[{ a: 'x', ai: false, id: e:1 }]"
*/
-- Enforced table with rows; both fields are Guaranteed by a migration.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE string;
DEFINE FIELD b ON p TYPE string;
CREATE p:1 SET a = '1', b = '2' RETURN NONE;
ALTER FIELD a ON p TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
ALTER FIELD b ON p TYPE int MIGRATE EXISTING ROWS AS b_int USING <int>$value FALLBACK 0;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
-- A row that misfits `a`, and one written in the old shape.
OPTION IMPORT;
CREATE p:2 SET a = 'x', b = 7 RETURN NONE;
CREATE p:3 SET a = '5', b = '6' RETURN NONE;
OPTION IMPORT = false;
'a' IN (INFO FOR TABLE p).migrations.guaranteed;
(INFO FOR TABLE p).migrations.generation;
SELECT id, a, type::is_int(a) AS ai, b, type::is_int(b) AS bi FROM p ORDER BY id;
-- The import is over: a write is coerced again.
CREATE p:4 SET a = 'y', b = 1;
CREATE p:4 SET a = 4, b = 4 RETURN NONE;
SELECT id, a, type::is_int(a) AS ai FROM p:4;
-- An enforced table with no rows.
DEFINE TABLE e SCHEMAFULL;
DEFINE FIELD a ON e TYPE int;
OPTION IMPORT;
CREATE e:1 SET a = 'x' RETURN NONE;
OPTION IMPORT = false;
SELECT id, a, type::is_int(a) AS ai FROM e;
(INFO FOR TABLE e).migrations.generation;
CREATE e:2 SET a = 'x';
-- A SCHEMALESS table with a migrated field.
DEFINE TABLE loose SCHEMALESS;
DEFINE FIELD a ON loose TYPE string;
CREATE loose:1 SET a = '1' RETURN NONE;
ALTER FIELD a ON loose TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
(INFO FOR TABLE loose).migrations.guaranteed;
OPTION IMPORT;
CREATE loose:2 SET a = 'x' RETURN NONE;
OPTION IMPORT = false;
(INFO FOR TABLE loose).migrations.guaranteed;
(INFO FOR TABLE loose).migrations.generation;
SELECT id, a, type::is_int(a) AS ai FROM loose ORDER BY id;
-- At compatibility level 3.
DEFINE DATABASE d3 COMPATIBILITY 3;
USE DB d3;
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE string;
DEFINE FIELD b ON p TYPE string;
CREATE p:1 SET a = '1', b = '2' RETURN NONE;
ALTER FIELD a ON p TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
ALTER FIELD b ON p TYPE int MIGRATE EXISTING ROWS AS b_int USING <int>$value FALLBACK 0;
(INFO FOR TABLE p).migrations.guaranteed;
OPTION IMPORT;
CREATE p:2 SET a = 'x', b = 7 RETURN NONE;
OPTION IMPORT = false;
'a' IN (INFO FOR TABLE p).migrations.guaranteed;
(INFO FOR TABLE p).migrations.generation;
SELECT id, a, type::is_int(a) AS ai, b, type::is_int(b) AS bi FROM p ORDER BY id;
CREATE p:3 SET a = 'y', b = 1;
DEFINE TABLE e SCHEMAFULL;
DEFINE FIELD a ON e TYPE int;
OPTION IMPORT;
CREATE e:1 SET a = 'x' RETURN NONE;
OPTION IMPORT = false;
SELECT id, a, type::is_int(a) AS ai FROM e;
