/**
[env]
remote-skip = "asserts the table migration job's progress before the job has run (`finished: false`, its lap and counts), which holds only because the embedded harness builds its datastores without maintenance tasks; a server wakes the job when the statement commits"

[test]
reason = "Changes 13-16: a MIGRATE with no TYPE change transforms (or repairs) every old row on any table and mints one generation; a definition restated unchanged and DEFINE FIELD IF NOT EXISTS on an existing field change nothing and mint nothing; REMOVE FIELD mints a removal step on an enforced table with rows, so the values disappear for every reader, and on every other table the stored values stay."

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
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "7"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "7"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ failures: 0, field: 'a', finished: false }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ a: 'DEFINE FIELD a ON p TYPE int PERMISSIONS FULL', b: 'DEFINE FIELD b ON p TYPE int PERMISSIONS FULL', c: 'DEFINE FIELD c ON p TYPE int PERMISSIONS FULL', x: 'DEFINE FIELD x ON p TYPE int PERMISSIONS FULL' }"

[[test.results]]
value = "7"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ failures: 0, field: 'a', finished: false }]"

[[test.results]]
value = "[{ a: 1, ai: true, b: 2, bi: true, c: 3, id: p:1, x: 4 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "8"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 1, b: 3, bi: true, c: 3, id: p:1, x: 4 }]"

[[test.results]]
error = "Cannot change field `x` of table `p`: the index `by_x` reads its values, which the removal clears: remove the index first"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "10"

[[test.results]]
value = "[{ field: 'a', generation: 6, name: 'a_int' }, { field: 'b', generation: 8, name: 'b_plus' }, { field: 'c', generation: 9, name: 'remove c after 8' }]"

[[test.results]]
value = "[{ a: 1, b: 3, id: p:1, x: 4 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 3, id: p:1, x: 4 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "13"

[[test.results]]
value = "NONE"

[[test.results]]
value = "13"

[[test.results]]
value = "[{ a: 1, b: 3, id: p:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ b: 3, id: p:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ b: 3, id: p:1 }]"

[[test.results]]
value = "16"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ b: 3, id: p:1 }, { a: 'new', b: 5, id: p:2 }]"

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
value = "{ a: 'DEFINE FIELD a ON e TYPE int PERMISSIONS FULL' }"

[[test.results]]
value = "4"

[[test.results]]
value = "NONE"

[[test.results]]
value = "5"

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
value = "[{ a: 1, ai: true, c: 3, id: loose:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: '1', astr: true, c: 3, id: loose:1 }]"

[[test.results]]
value = "2"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.database == 'd3'"

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
value = "[{ a: 1, ai: true, c: 3, id: p:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, c: 3, id: p:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: '1', astr: true, c: 3, id: p:1 }]"

[[test.results]]
value = "7"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ a: 'DEFINE FIELD a ON p TYPE string PERMISSIONS FULL', c: 'DEFINE FIELD c ON p TYPE none | int PERMISSIONS FULL' }"

[[test.results]]
value = "7"

[[test.results]]
value = "[]"
*/
-- Enforced table with rows.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE string;
DEFINE FIELD b ON p TYPE int;
DEFINE FIELD c ON p TYPE int;
DEFINE FIELD x ON p TYPE int;
DEFINE INDEX by_x ON p FIELDS x;
CREATE p:1 SET a = '1', b = 2, c = 3, x = 4 RETURN NONE;
ALTER FIELD a ON p TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
-- 14 A definition restated unchanged.
DEFINE FIELD OVERWRITE a ON p TYPE int;
ALTER FIELD a ON p TYPE int;
DEFINE FIELD OVERWRITE b ON p TYPE int;
ALTER FIELD b ON p TYPE int;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
(INFO FOR TABLE p).migrations.verification;
-- 15 DEFINE FIELD IF NOT EXISTS on an existing field does nothing, whatever
-- the definition it carries.
DEFINE FIELD IF NOT EXISTS a ON p TYPE int;
DEFINE FIELD IF NOT EXISTS a ON p TYPE string;
DEFINE FIELD IF NOT EXISTS b ON p TYPE float;
(INFO FOR TABLE p).fields;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
(INFO FOR TABLE p).migrations.verification;
SELECT *, type::is_int(a) AS ai, type::is_int(b) AS bi FROM p;
-- 13 MIGRATE with no TYPE change.
ALTER FIELD b ON p MIGRATE EXISTING ROWS AS b_plus USING $value + 1 FALLBACK 0;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
SELECT *, type::is_int(b) AS bi FROM p;
-- 16 REMOVE FIELD mints a removal step; an index that reads the field forbids it.
REMOVE FIELD x ON p;
REMOVE FIELD c ON p;
REMOVE FIELD IF EXISTS nope ON p;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.migrations;
SELECT * FROM p;
DEFINE FIELD c ON p TYPE option<int>;
SELECT * FROM p;
REMOVE INDEX by_x ON p;
REMOVE FIELD x ON p;
(INFO FOR TABLE p).migrations.generation;
-- DEFINE FIELD IF NOT EXISTS runs no migration on a field that exists.
DEFINE FIELD IF NOT EXISTS b ON p TYPE int MIGRATE EXISTING ROWS AS b_twice USING $value * 2 FALLBACK 0;
(INFO FOR TABLE p).migrations.generation;
SELECT * FROM p;
-- A field removed while rows are behind its step loses the migrated value
-- too, and a field of the same name defined afterwards starts empty.
REMOVE FIELD a ON p;
SELECT * FROM p;
DEFINE FIELD a ON p TYPE option<string>;
SELECT * FROM p;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
CREATE p:2 SET a = 'new', b = 5 RETURN NONE;
SELECT * FROM p ORDER BY id;
-- An enforced table with no rows mints nothing for a removal.
DEFINE TABLE e SCHEMAFULL;
DEFINE FIELD a ON e TYPE int;
DEFINE FIELD b ON e TYPE int;
REMOVE FIELD b ON e;
DEFINE FIELD OVERWRITE a ON e TYPE int;
DEFINE FIELD IF NOT EXISTS a ON e TYPE string;
(INFO FOR TABLE e).fields;
(INFO FOR TABLE e).migrations.generation;
ALTER FIELD a ON e MIGRATE EXISTING ROWS AS a_plus USING $value + 1 FALLBACK 0;
(INFO FOR TABLE e).migrations.generation;
-- A SCHEMALESS table keeps the values of a removed field, and a MIGRATE of
-- a field it never retyped repairs the old rows.
DEFINE TABLE loose SCHEMALESS;
DEFINE FIELD a ON loose TYPE int;
DEFINE FIELD c ON loose TYPE int;
CREATE loose:1 SET a = 1, c = 3 RETURN NONE;
ALTER FIELD a ON loose TYPE string;
REMOVE FIELD c ON loose;
DEFINE FIELD c ON loose TYPE option<int>;
SELECT *, type::is_int(a) AS ai FROM loose;
ALTER FIELD a ON loose MIGRATE EXISTING ROWS AS a_string USING <string>$value FALLBACK '';
SELECT *, type::is_string(a) AS astr FROM loose;
(INFO FOR TABLE loose).migrations.generation;
(INFO FOR TABLE loose).migrations.guaranteed;
-- At compatibility level 3 the same holds.
DEFINE DATABASE d3 COMPATIBILITY 3;
USE DB d3;
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE int;
DEFINE FIELD c ON p TYPE int;
CREATE p:1 SET a = 1, c = 3 RETURN NONE;
ALTER FIELD a ON p TYPE string;
REMOVE FIELD c ON p;
SELECT *, type::is_int(a) AS ai FROM p;
DEFINE FIELD c ON p TYPE option<int>;
SELECT * FROM p;
ALTER FIELD a ON p MIGRATE EXISTING ROWS AS a_string USING <string>$value FALLBACK '';
SELECT *, type::is_string(a) AS astr FROM p;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
DEFINE FIELD OVERWRITE a ON p TYPE string;
DEFINE FIELD IF NOT EXISTS a ON p TYPE int;
(INFO FOR TABLE p).fields;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
