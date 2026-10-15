/**
[test]
reason = "Changes 18-20: a VALUE, DEFAULT, DEFAULT ALWAYS or READONLY clause added, changed or dropped mints no generation on any table, keeps a Guaranteed field Guaranteed and leaves every old row reading exactly as before; the new clause applies to a row written afterwards, and a migration applies none of them."

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

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "10"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ d2: 1, g: 1, id: p:1, r: 1, v: 1, w: 2 }, { d: 2, d2: 2, da: 2, g: 2, id: p:2, r: 2, v: 2, w: 4 }]"

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
value = "{ d: 'DEFINE FIELD d ON p TYPE none | int DEFAULT 5 PERMISSIONS FULL', d2: 'DEFINE FIELD d2 ON p TYPE none | int DEFAULT 9 PERMISSIONS FULL', da: 'DEFINE FIELD da ON p TYPE none | int DEFAULT ALWAYS 6 PERMISSIONS FULL', g: 'DEFINE FIELD g ON p TYPE int DEFAULT 0 READONLY VALUE $value + 1000 PERMISSIONS FULL', r: 'DEFINE FIELD r ON p TYPE int READONLY PERMISSIONS FULL', v: 'DEFINE FIELD v ON p TYPE int VALUE $value + 100 PERMISSIONS FULL', w: 'DEFINE FIELD w ON p TYPE int VALUE $value * 3 PERMISSIONS FULL' }"

[[test.results]]
value = "10"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ d2: 1, g: 1, id: p:1, r: 1, v: 1, w: 2 }, { d: 2, d2: 2, da: 2, g: 2, id: p:2, r: 2, v: 2, w: 4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ d: 5, d2: 9, da: 6, g: 1003, id: p:3, r: 3, v: 103, w: 9 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ d: NONE, d2: 1, da: 6, g: 1, id: p:1, r: 1, v: 101, w: 6 }]"

[[test.results]]
error = "Found changed value for field `r`, with record `p:2`, but field is readonly"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ da: 6, id: p:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: p:1, r: 11, v: 102 }, { id: p:2, r: 12, v: 103 }, { id: p:3, r: 13, v: 104 }]"

[[test.results]]
value = "12"

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
value = "{ d: 'DEFINE FIELD d ON p TYPE none | int PERMISSIONS FULL', d2: 'DEFINE FIELD d2 ON p TYPE none | int PERMISSIONS FULL', da: 'DEFINE FIELD da ON p TYPE none | int PERMISSIONS FULL', g: 'DEFINE FIELD g ON p TYPE int PERMISSIONS FULL', r: 'DEFINE FIELD r ON p TYPE int PERMISSIONS FULL', v: 'DEFINE FIELD v ON p TYPE int PERMISSIONS FULL', w: 'DEFINE FIELD w ON p TYPE int PERMISSIONS FULL' }"

[[test.results]]
value = "12"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ d2: 1, da: 6, g: 1, id: p:1, r: 11, v: 102, w: 6 }, { d: 2, d2: 2, da: 6, g: 2, id: p:2, r: 12, v: 103, w: 12 }, { d: 5, d2: 9, da: 6, g: 1003, id: p:3, r: 13, v: 104, w: 9 }]"

[[test.results]]
value = "[{ g: 4, id: p:4, r: 4, v: 4, w: 4 }]"

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

[[test.results]]
value = "NONE"

[[test.results]]
value = "5"

[[test.results]]
value = "[{ d: 5, da: 6, id: e:1, r: 1, v: 101 }]"

[[test.results]]
value = "[{ d: 5, da: 6, id: e:1, r: 1, v: 102 }]"

[[test.results]]
error = "Found changed value for field `r`, with record `e:1`, but field is readonly"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: loose:1, r: 1, v: 1 }]"

[[test.results]]
value = "[{ d: 5, id: loose:2, r: 2, v: 102 }]"

[[test.results]]
error = "Found changed value for field `r`, with record `loose:1`, but field is readonly"

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
value = "NONE"

[[test.results]]
value = "[{ id: p:1, r: 1, v: 1 }]"

[[test.results]]
value = "[{ d: 5, da: 6, id: p:2, r: 2, v: 102 }]"

[[test.results]]
value = "[{ da: 6, id: p:1, r: 1, v: 101 }]"

[[test.results]]
error = "Found changed value for field `r`, with record `p:2`, but field is readonly"

[[test.results]]
value = "{ d: 'DEFINE FIELD d ON p TYPE none | int DEFAULT 5 PERMISSIONS FULL', da: 'DEFINE FIELD da ON p TYPE none | int DEFAULT ALWAYS 6 PERMISSIONS FULL', r: 'DEFINE FIELD r ON p TYPE int READONLY PERMISSIONS FULL', v: 'DEFINE FIELD v ON p TYPE int VALUE $value + 100 PERMISSIONS FULL' }"
*/
-- Enforced table with rows. `g` is Guaranteed by a migration, so the state a
-- clause change must keep is visible.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD v ON p TYPE int;
DEFINE FIELD w ON p TYPE int VALUE $value * 2;
DEFINE FIELD d ON p TYPE option<int>;
DEFINE FIELD d2 ON p TYPE option<int> DEFAULT 1;
DEFINE FIELD da ON p TYPE option<int>;
DEFINE FIELD r ON p TYPE int;
DEFINE FIELD g ON p TYPE string;
CREATE p:1 SET v = 1, w = 1, r = 1, g = '1' RETURN NONE;
CREATE p:2 SET v = 2, w = 2, d = 2, d2 = 2, da = 2, r = 2, g = '2' RETURN NONE;
ALTER FIELD g ON p TYPE int MIGRATE EXISTING ROWS AS g_int USING <int>$value FALLBACK 0;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
SELECT * FROM p ORDER BY id;
-- 18 VALUE added, changed; 19 DEFAULT added, changed, DEFAULT ALWAYS added;
-- 20 READONLY added.
ALTER FIELD v ON p VALUE $value + 100;
ALTER FIELD w ON p VALUE $value * 3;
ALTER FIELD d ON p DEFAULT 5;
ALTER FIELD d2 ON p DEFAULT 9;
ALTER FIELD da ON p DEFAULT ALWAYS 6;
ALTER FIELD r ON p READONLY;
ALTER FIELD g ON p VALUE $value + 1000 DEFAULT 0 READONLY;
(INFO FOR TABLE p).fields;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
-- Every old row reads as it did.
SELECT * FROM p ORDER BY id;
-- A row written afterwards takes the new clauses.
CREATE p:3 SET v = 3, w = 3, r = 3, g = 3 RETURN NONE;
SELECT * FROM p:3;
-- So does an old row that is written.
UPDATE p:1 SET v = 1 RETURN NONE;
SELECT id, v, w, d, d2, da, r, g FROM p:1;
UPDATE p:2 SET r = 9;
UPDATE p:2 SET da = NONE RETURN NONE;
SELECT id, da FROM p:2;
-- A migration applies none of them: not VALUE, not DEFAULT, and READONLY
-- does not stop it.
ALTER FIELD r ON p MIGRATE EXISTING ROWS AS r_plus USING $value + 10 FALLBACK 0;
ALTER FIELD v ON p MIGRATE EXISTING ROWS AS v_plus USING $value + 1 FALLBACK 0;
SELECT id, r, v FROM p ORDER BY id;
(INFO FOR TABLE p).migrations.generation;
-- Dropped: every clause goes, the old rows still read as they did.
ALTER FIELD v ON p DROP VALUE;
ALTER FIELD w ON p DROP VALUE;
ALTER FIELD d ON p DROP DEFAULT;
ALTER FIELD d2 ON p DROP DEFAULT;
ALTER FIELD da ON p DROP DEFAULT;
ALTER FIELD r ON p DROP READONLY;
ALTER FIELD g ON p DROP VALUE DROP DEFAULT DROP READONLY;
(INFO FOR TABLE p).fields;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
SELECT * FROM p ORDER BY id;
CREATE p:4 SET v = 4, w = 4, r = 4, g = 4;
-- Enforced table with no rows.
DEFINE TABLE e SCHEMAFULL;
DEFINE FIELD v ON e TYPE int;
DEFINE FIELD d ON e TYPE option<int>;
DEFINE FIELD da ON e TYPE option<int>;
DEFINE FIELD r ON e TYPE int;
ALTER FIELD v ON e VALUE $value + 100;
ALTER FIELD d ON e DEFAULT 5;
ALTER FIELD da ON e DEFAULT ALWAYS 6;
ALTER FIELD r ON e READONLY;
(INFO FOR TABLE e).migrations.generation;
CREATE e:1 SET v = 1, r = 1;
UPDATE e:1 SET v = 2, da = NONE;
UPDATE e:1 SET r = 2;
-- SCHEMALESS table with rows.
DEFINE TABLE loose SCHEMALESS;
DEFINE FIELD v ON loose TYPE int;
DEFINE FIELD d ON loose TYPE option<int>;
DEFINE FIELD r ON loose TYPE int;
CREATE loose:1 SET v = 1, r = 1 RETURN NONE;
ALTER FIELD v ON loose VALUE $value + 100;
ALTER FIELD d ON loose DEFAULT 5;
ALTER FIELD r ON loose READONLY;
SELECT * FROM loose;
CREATE loose:2 SET v = 2, r = 2;
UPDATE loose:1 SET r = 5;
-- At compatibility level 3.
DEFINE DATABASE d3 COMPATIBILITY 3;
USE DB d3;
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD v ON p TYPE int;
DEFINE FIELD d ON p TYPE option<int>;
DEFINE FIELD da ON p TYPE option<int>;
DEFINE FIELD r ON p TYPE int;
CREATE p:1 SET v = 1, r = 1 RETURN NONE;
ALTER FIELD v ON p VALUE $value + 100;
ALTER FIELD d ON p DEFAULT 5;
ALTER FIELD da ON p DEFAULT ALWAYS 6;
ALTER FIELD r ON p READONLY;
SELECT * FROM p;
CREATE p:2 SET v = 2, r = 2;
UPDATE p:1 SET v = 1;
UPDATE p:2 SET r = 5;
(INFO FOR TABLE p).fields;
