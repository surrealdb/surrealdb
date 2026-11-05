/**
[test]
reason = "Changes 21 and 22: an ALTER FIELD that changes only PERMISSIONS or only COMMENT is accepted by every table, mints no generation, keeps a Guaranteed field Guaranteed and leaves every old row reading as it did."

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
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "6"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 1, b: 2, c: 3, id: p:1 }, { a: 4, b: 5, id: p:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ a: 'DEFINE FIELD a ON p TYPE int PERMISSIONS FOR select WHERE $value > 0, FOR create, update NONE', b: 'DEFINE FIELD b ON p TYPE int PERMISSIONS FOR select, create, update NONE', c: 'DEFINE FIELD c ON p TYPE none | int PERMISSIONS FOR select, create, update NONE' }"

[[test.results]]
value = "6"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 1, b: 2, c: 3, id: p:1 }, { a: 4, b: 5, id: p:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ a: "DEFINE FIELD a ON p TYPE int COMMENT 'the a' PERMISSIONS FOR select WHERE $value > 0, FOR create, update NONE", b: "DEFINE FIELD b ON p TYPE int COMMENT 'the b' PERMISSIONS FOR select, create, update NONE", c: "DEFINE FIELD c ON p TYPE none | int COMMENT 'the c' PERMISSIONS FOR select, create, update NONE" }"""

[[test.results]]
value = "6"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 1, b: 2, c: 3, id: p:1 }, { a: 4, b: 5, id: p:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ a: 'DEFINE FIELD a ON p TYPE int PERMISSIONS FULL', b: 'DEFINE FIELD b ON p TYPE int PERMISSIONS FULL', c: 'DEFINE FIELD c ON p TYPE none | int PERMISSIONS FULL' }"

[[test.results]]
value = "6"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 1, b: 2, c: 3, id: p:1 }, { a: 4, b: 5, id: p:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ a: "DEFINE FIELD a ON e TYPE int COMMENT 'the a' PERMISSIONS FOR select, create, update NONE" }"""

[[test.results]]
value = "2"

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
value = """{ a: "DEFINE FIELD a ON loose TYPE int COMMENT 'the a' PERMISSIONS FOR select, create, update NONE" }"""

[[test.results]]
value = "[{ a: 1, id: loose:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.database == 'd3'"

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
value = """{ a: "DEFINE FIELD a ON p TYPE int COMMENT 'the a' PERMISSIONS FOR select, create, update NONE" }"""

[[test.results]]
value = "4"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 1, id: p:1 }]"
*/
-- Enforced table with rows; `a` is Guaranteed by a migration.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE string;
DEFINE FIELD b ON p TYPE int;
DEFINE FIELD c ON p TYPE option<int>;
CREATE p:1 SET a = '1', b = 2, c = 3 RETURN NONE;
CREATE p:2 SET a = '4', b = 5 RETURN NONE;
ALTER FIELD a ON p TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
SELECT * FROM p ORDER BY id;
-- 21 PERMISSIONS only.
ALTER FIELD a ON p PERMISSIONS FOR select WHERE $value > 0;
ALTER FIELD b ON p PERMISSIONS NONE;
ALTER FIELD c ON p PERMISSIONS FOR create, update NONE;
(INFO FOR TABLE p).fields;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
SELECT * FROM p ORDER BY id;
-- 22 COMMENT only.
ALTER FIELD a ON p COMMENT 'the a';
ALTER FIELD b ON p COMMENT 'the b';
ALTER FIELD c ON p COMMENT 'the c';
(INFO FOR TABLE p).fields;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
SELECT * FROM p ORDER BY id;
-- Both again, back to what they were.
ALTER FIELD a ON p PERMISSIONS FULL DROP COMMENT;
ALTER FIELD b ON p PERMISSIONS FULL DROP COMMENT;
ALTER FIELD c ON p PERMISSIONS FULL DROP COMMENT;
(INFO FOR TABLE p).fields;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
SELECT * FROM p ORDER BY id;
-- An enforced table with no rows.
DEFINE TABLE e SCHEMAFULL;
DEFINE FIELD a ON e TYPE int;
ALTER FIELD a ON e PERMISSIONS NONE;
ALTER FIELD a ON e COMMENT 'the a';
(INFO FOR TABLE e).fields;
(INFO FOR TABLE e).migrations.generation;
-- A SCHEMALESS table with rows.
DEFINE TABLE loose SCHEMALESS;
DEFINE FIELD a ON loose TYPE int;
CREATE loose:1 SET a = 1 RETURN NONE;
ALTER FIELD a ON loose PERMISSIONS NONE;
ALTER FIELD a ON loose COMMENT 'the a';
(INFO FOR TABLE loose).fields;
SELECT * FROM loose;
-- At compatibility level 3.
DEFINE DATABASE d3 COMPATIBILITY 3;
USE DB d3;
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE string;
CREATE p:1 SET a = '1' RETURN NONE;
ALTER FIELD a ON p TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
ALTER FIELD a ON p PERMISSIONS NONE;
ALTER FIELD a ON p COMMENT 'the a';
(INFO FOR TABLE p).fields;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
SELECT * FROM p;
