/**
[test]
reason = "Change 17: an ASSERT added, changed or dropped is accepted by every table, with or without rows and at either compatibility level; it mints no generation, keeps a Guaranteed field Guaranteed, leaves the rows that violate it readable as they were, and holds every write that follows."

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ a: 'DEFINE FIELD a ON p TYPE int ASSERT $value > 3 PERMISSIONS FULL', b: 'DEFINE FIELD b ON p TYPE int ASSERT $value > 1 PERMISSIONS FULL', g: 'DEFINE FIELD g ON p TYPE int ASSERT $value < 100 PERMISSIONS FULL' }"

[[test.results]]
value = "6"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 1, b: 1, g: 1, gi: true, id: p:1 }, { a: 5, b: 2, g: 2, gi: true, id: p:2 }]"

[[test.results]]
error = "Found 1 for field `a`, with record `p:3`, but field must conform to: $value > 3"

[[test.results]]
error = "Found 1 for field `b`, with record `p:4`, but field must conform to: $value > 1"

[[test.results]]
error = "Found 100 for field `g`, with record `p:5`, but field must conform to: $value < 100"

[[test.results]]
value = "[]"

[[test.results]]
error = "Found 1 for field `a`, with record `p:1`, but field must conform to: $value > 3"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 8, b: 7, g: 7, id: p:1 }, { a: 5, b: 2, g: 2, id: p:2 }, { a: 9, b: 5, g: 1, id: p:6 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ a: 'DEFINE FIELD a ON p TYPE int PERMISSIONS FULL', b: 'DEFINE FIELD b ON p TYPE int PERMISSIONS FULL', g: 'DEFINE FIELD g ON p TYPE int ASSERT $value < 100 PERMISSIONS FULL' }"

[[test.results]]
value = "6"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ a: 'DEFINE FIELD a ON e TYPE int ASSERT $value > 3 PERMISSIONS FULL' }"

[[test.results]]
value = "2"

[[test.results]]
error = "Found 1 for field `a`, with record `e:1`, but field must conform to: $value > 3"

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
value = "[{ a: 1, id: loose:1 }]"

[[test.results]]
error = "Found 1 for field `a`, with record `loose:2`, but field must conform to: $value > 3"

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
value = "[{ a: 1, id: p:1 }]"

[[test.results]]
error = "Found 1 for field `a`, with record `p:2`, but field must conform to: $value > 3"

[[test.results]]
value = "[]"

[[test.results]]
error = "Found 1 for field `a`, with record `p:1`, but field must conform to: $value > 3"
*/
-- Enforced table with rows, one of which violates the new assertions.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE int;
DEFINE FIELD b ON p TYPE int ASSERT $value > 0;
DEFINE FIELD g ON p TYPE string;
CREATE p:1 SET a = 1, b = 1, g = '1' RETURN NONE;
CREATE p:2 SET a = 5, b = 2, g = '2' RETURN NONE;
ALTER FIELD g ON p TYPE int MIGRATE EXISTING ROWS AS g_int USING <int>$value FALLBACK 0;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
-- Added, changed, and added to a Guaranteed field.
ALTER FIELD a ON p ASSERT $value > 3;
ALTER FIELD b ON p ASSERT $value > 1;
ALTER FIELD g ON p ASSERT $value < 100;
(INFO FOR TABLE p).fields;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
SELECT id, a, b, g, type::is_int(g) AS gi FROM p ORDER BY id;
-- A row written afterwards is held to the new assertions.
CREATE p:3 SET a = 1, b = 5, g = 1;
CREATE p:4 SET a = 9, b = 1, g = 1;
CREATE p:5 SET a = 9, b = 5, g = 100;
CREATE p:6 SET a = 9, b = 5, g = 1 RETURN NONE;
-- A write to the old row that violates them is held to them as well.
UPDATE p:1 SET g = 7;
UPDATE p:1 SET a = 8, b = 7, g = 7 RETURN NONE;
SELECT id, a, b, g FROM p ORDER BY id;
-- Dropped.
ALTER FIELD a ON p DROP ASSERT;
DEFINE FIELD OVERWRITE b ON p TYPE int;
CREATE p:7 SET a = 1, b = 1, g = 1 RETURN NONE;
(INFO FOR TABLE p).fields;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
-- An enforced table with no rows.
DEFINE TABLE e SCHEMAFULL;
DEFINE FIELD a ON e TYPE int;
ALTER FIELD a ON e ASSERT $value > 3;
(INFO FOR TABLE e).fields;
(INFO FOR TABLE e).migrations.generation;
CREATE e:1 SET a = 1;
CREATE e:2 SET a = 4 RETURN NONE;
-- A SCHEMALESS table.
DEFINE TABLE loose SCHEMALESS;
DEFINE FIELD a ON loose TYPE int;
CREATE loose:1 SET a = 1 RETURN NONE;
ALTER FIELD a ON loose ASSERT $value > 3;
SELECT * FROM loose;
CREATE loose:2 SET a = 1;
-- At compatibility level 3.
DEFINE DATABASE d3 COMPATIBILITY 3;
USE DB d3;
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE int;
CREATE p:1 SET a = 1 RETURN NONE;
ALTER FIELD a ON p ASSERT $value > 3;
SELECT * FROM p;
CREATE p:2 SET a = 1;
CREATE p:3 SET a = 4 RETURN NONE;
UPDATE p:1 SET a = 1;
