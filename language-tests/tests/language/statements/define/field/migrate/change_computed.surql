/**
[test]
reason = "Changes 11 and 12: a COMPUTED field made stored is a new field (an enforced table with rows refuses one that does not accept NONE unless it carries MIGRATE), and on an enforced table with rows a step first clears its path of whatever values were kept beneath the computed field; a stored field made COMPUTED mints a removal step on an enforced table with rows, so the stored values disappear for every reader at once; every other table accepts both, mints nothing for either and keeps the stored values."

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
value = "[{ a: 1, c: 2, d: 2, id: p:1, s: 10 }, { a: 2, c: 4, d: 3, id: p:2, s: 20 }]"

[[test.results]]
value = "3"

[[test.results]]
error = "Cannot change field `c` of table `p`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the field's TYPE does not accept NONE: add MIGRATE EXISTING ROWS to say what existing rows hold"

[[test.results]]
value = "'DEFINE FIELD c ON p COMPUTED a * 2 PERMISSIONS FULL'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "8"

[[test.results]]
value = "[{ field: 'c', generation: 4, name: 'remove c after 3' }, { field: 'd', generation: 6, name: 'remove d after 5' }, { field: 'd', generation: 7, name: 'd_stored' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ failures: 0, field: 'c', finished: false }, { failures: 0, field: 'd', finished: false }]"

[[test.results]]
value = "[{ a: 1, d: 100, id: p:1, s: 10 }, { a: 2, d: 100, id: p:2, s: 20 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "10"

[[test.results]]
value = "[{ field: 'c', generation: 4, name: 'remove c after 3' }, { field: 'd', generation: 6, name: 'remove d after 5' }, { field: 'd', generation: 7, name: 'd_stored' }, { field: 's', generation: 9, name: 'remove s after 8' }]"

[[test.results]]
value = "[{ s: 100 }, { s: 200 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, d: 100, id: p:1 }, { a: 2, d: 100, id: p:2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 1, d: 100, id: p:1 }, { a: 2, d: 100, id: p:2 }, { a: 3, d: 5, id: p:3, s: 30 }]"

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
value = "{ a: 'DEFINE FIELD a ON e TYPE int PERMISSIONS FULL', c: 'DEFINE FIELD c ON e TYPE int PERMISSIONS FULL', s: 'DEFINE FIELD s ON e COMPUTED a * 100 PERMISSIONS FULL' }"

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
value = "[{ a: 1, id: loose:1, s: 100 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: loose:1, s: 10 }]"

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
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: p:1, s: 100 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: p:1, s: 10 }]"
*/
-- Enforced table with rows.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE int;
DEFINE FIELD c ON p COMPUTED a * 2;
DEFINE FIELD d ON p COMPUTED a + 1;
DEFINE FIELD s ON p TYPE int;
CREATE p:1 SET a = 1, s = 10 RETURN NONE;
CREATE p:2 SET a = 2, s = 20 RETURN NONE;
SELECT * FROM p ORDER BY id;
(INFO FOR TABLE p).migrations.generation;
-- 11 COMPUTED made stored.
DEFINE FIELD OVERWRITE c ON p TYPE int;
(INFO FOR TABLE p).fields.c;
DEFINE FIELD OVERWRITE c ON p TYPE option<int>;
DEFINE FIELD OVERWRITE d ON p TYPE int MIGRATE EXISTING ROWS AS d_stored USING 100;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.migrations;
(INFO FOR TABLE p).migrations.guaranteed;
(INFO FOR TABLE p).migrations.verification;
SELECT * FROM p ORDER BY id;
-- 12 A stored field made COMPUTED: its values are removed for every reader
-- by a step.
DEFINE FIELD OVERWRITE s ON p COMPUTED a * 100;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.migrations;
SELECT s FROM p ORDER BY id;
-- Stored again, the field holds nothing for a row that existed before the
-- step.
DEFINE FIELD OVERWRITE s ON p TYPE option<int>;
SELECT * FROM p ORDER BY id;
CREATE p:3 SET a = 3, s = 30, d = 5 RETURN NONE;
SELECT * FROM p ORDER BY id;
-- An enforced table with no rows has no values to remove and nothing to fill.
DEFINE TABLE e SCHEMAFULL;
DEFINE FIELD a ON e TYPE int;
DEFINE FIELD c ON e COMPUTED a * 2;
DEFINE FIELD s ON e TYPE int;
DEFINE FIELD OVERWRITE c ON e TYPE int;
DEFINE FIELD OVERWRITE s ON e COMPUTED a * 100;
(INFO FOR TABLE e).fields;
(INFO FOR TABLE e).migrations.generation;
-- A SCHEMALESS table accepts both and keeps the stored values.
DEFINE TABLE loose SCHEMALESS;
DEFINE FIELD c ON loose COMPUTED a * 2;
DEFINE FIELD s ON loose TYPE int;
CREATE loose:1 SET a = 1, s = 10 RETURN NONE;
DEFINE FIELD OVERWRITE c ON loose TYPE int;
DEFINE FIELD OVERWRITE s ON loose COMPUTED a * 100;
SELECT * FROM loose;
DEFINE FIELD OVERWRITE s ON loose TYPE option<int>;
SELECT * FROM loose;
-- At compatibility level 3 no table is enforced.
DEFINE DATABASE d3 COMPATIBILITY 3;
USE DB d3;
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE int;
DEFINE FIELD c ON p COMPUTED a * 2;
DEFINE FIELD s ON p TYPE int;
CREATE p:1 SET a = 1, s = 10 RETURN NONE;
DEFINE FIELD OVERWRITE c ON p TYPE int;
DEFINE FIELD OVERWRITE s ON p COMPUTED a * 100;
SELECT * FROM p;
DEFINE FIELD OVERWRITE s ON p TYPE option<int>;
SELECT * FROM p;
