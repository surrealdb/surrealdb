/**
[test]
reason = "A view defined over a source whose rows are behind a migration step is built from the migrated values and maintained from them: the rows it selects, the groups it keys, the sums it adds and the filters it applies see migrated values, and a write to a row behind the step adds and removes the migrated value from its group, not the stored one. A view's source takes no further migration while the view exists, and takes it again when the view is removed."

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
value = "[]"

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
value = "[{ a: 5, g: 'x', id: v:1 }, { a: 7, g: 'x', id: v:2 }, { a: 100, g: 'y', id: v:3 }, { a: 3, g: 'y', id: v:4 }]"

[[test.results]]
value = "[true, true, true, true]"

[[test.results]]
value = "[{ a: 7, g: 'x', id: big:2 }, { a: 100, g: 'y', id: big:3 }]"

[[test.results]]
value = "[{ c: 2, g: 'x', id: tot:['x'], s: 12f }, { c: 2, g: 'y', id: tot:['y'], s: 103f }]"

[[test.results]]
value = "[by_a:[3], by_a:[5], by_a:[7], by_a:[100]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ c: 2, g: 'x', id: tot:['x'], s: 57f }, { c: 2, g: 'y', id: tot:['y'], s: 103f }]"

[[test.results]]
value = "[{ a: 50, g: 'x', id: big:1 }, { a: 7, g: 'x', id: big:2 }, { a: 100, g: 'y', id: big:3 }]"

[[test.results]]
value = "[by_a:[3], by_a:[7], by_a:[50], by_a:[100]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ c: 1, g: 'x', id: tot:['x'], s: 50f }, { c: 2, g: 'y', id: tot:['y'], s: 103f }]"

[[test.results]]
value = "[{ a: 50, g: 'x', id: big:1 }, { a: 100, g: 'y', id: big:3 }]"

[[test.results]]
value = "[{ a: 50, g: 'x', id: v:1 }, { a: 100, g: 'y', id: v:3 }, { a: 3, g: 'y', id: v:4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ c: 2, g: 'x', id: tot:['x'], s: 150f }, { c: 1, g: 'y', id: tot:['y'], s: 3f }]"

[[test.results]]
value = "[{ a: 50, g: 'x', id: big:1 }, { a: 100, g: 'x', id: big:3 }]"

[[test.results]]
value = "[by_a:[3], by_a:[50], by_a:[100]]"

[[test.results]]
error = "Cannot migrate field `a` of table `src`: the view `big` selects from the table"

[[test.results]]
error = "Cannot migrate field `g` of table `src`: the view `big` selects from the table"

[[test.results]]
error = "Cannot migrate field `x` of table `v`: a view's rows skip their field definitions"

[[test.results]]
error = "Cannot migrate table `src`: the view `big` selects from the table"

[[test.results]]
value = "[{ field: 'a', generation: 4, name: 'a_int' }]"

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
value = "[{ field: 'a', generation: 4, name: 'a_int' }, { field: 'a', generation: 6, name: 'a_plus' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 51, g: 'x', id: v:1 }, { a: 101, g: 'x', id: v:3 }, { a: 4, g: 'y', id: v:4 }]"

[[test.results]]
value = "[{ c: 2, g: 'x', id: tot:['x'], s: 152f }, { c: 1, g: 'y', id: tot:['y'], s: 4f }]"

[[test.results]]
value = "[{ a: 51, g: 'x', id: src:1 }, { a: 101, g: 'x', id: src:3 }, { a: 4, g: 'y', id: src:4 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'lvl3', namespace: 'test' }"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, id: v:1 }, { a: 100, id: v:2 }]"

[[test.results]]
value = "[{ id: tot:[], s: 105f }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: tot:[], s: 6f }]"

[[test.results]]
error = "Cannot migrate field `a` of table `src`: the view `tot` selects from the table"
*/
DEFINE TABLE src SCHEMAFULL;
DEFINE FIELD a ON src TYPE string;
DEFINE FIELD g ON src TYPE string;
CREATE src:1 SET a = '5', g = 'x' RETURN NONE;
CREATE src:2 SET a = '7', g = 'x' RETURN NONE;
CREATE src:3 SET a = 'junk', g = 'y' RETURN NONE;
ALTER FIELD a ON src TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 100;
CREATE src:4 SET a = 3, g = 'y' RETURN NONE;
DEFINE TABLE v AS SELECT a, g FROM src;
DEFINE TABLE big AS SELECT a, g FROM src WHERE a > 6;
DEFINE TABLE tot AS SELECT g, math::sum(a) AS s, count() AS c FROM src GROUP BY g;
DEFINE TABLE by_a AS SELECT a, count() AS c FROM src GROUP BY a;
SELECT * FROM v ORDER BY id;
SELECT VALUE type::is_int(a) FROM v ORDER BY id;
SELECT * FROM big ORDER BY id;
SELECT * FROM tot ORDER BY g;
SELECT VALUE id FROM by_a ORDER BY id;
-- A write to a row behind the step moves its migrated value, not its stored one.
UPDATE src:1 SET a = 50 RETURN NONE;
SELECT * FROM tot ORDER BY g;
SELECT * FROM big ORDER BY id;
SELECT VALUE id FROM by_a ORDER BY id;
DELETE src:2;
SELECT * FROM tot ORDER BY g;
SELECT * FROM big ORDER BY id;
SELECT * FROM v ORDER BY id;
UPDATE src:3 SET g = 'x' RETURN NONE;
SELECT * FROM tot ORDER BY g;
SELECT * FROM big ORDER BY id;
SELECT VALUE id FROM by_a ORDER BY id;
-- The source takes no migration while a view reads it, and the view's own rows take none.
ALTER FIELD a ON src TYPE int MIGRATE EXISTING ROWS AS a_plus USING $value + 1 FALLBACK 0;
ALTER FIELD g ON src TYPE string MIGRATE EXISTING ROWS AS g_up USING string::uppercase($value) FALLBACK '';
DEFINE FIELD x ON v TYPE int MIGRATE EXISTING ROWS AS x_m USING 1 FALLBACK 1;
ALTER TABLE src MIGRATE EXISTING ROWS AS drop_extra REMOVE UNDECLARED FIELDS;
(INFO FOR TABLE src).migrations.migrations;
-- With the views removed it takes it again, and a view defined after it is built from both steps.
REMOVE TABLE v;
REMOVE TABLE big;
REMOVE TABLE tot;
REMOVE TABLE by_a;
ALTER FIELD a ON src TYPE int MIGRATE EXISTING ROWS AS a_plus USING $value + 1 FALLBACK 0;
(INFO FOR TABLE src).migrations.migrations;
DEFINE TABLE v AS SELECT a, g FROM src;
DEFINE TABLE tot AS SELECT g, math::sum(a) AS s, count() AS c FROM src GROUP BY g;
SELECT * FROM v ORDER BY id;
SELECT * FROM tot ORDER BY g;
SELECT * FROM src ORDER BY id;
-- The same at compatibility level 3.
DEFINE DATABASE lvl3 COMPATIBILITY 3;
USE DB lvl3;
DEFINE TABLE src SCHEMAFULL;
DEFINE FIELD a ON src TYPE string;
CREATE src:1 SET a = '5' RETURN NONE;
CREATE src:2 SET a = 'junk' RETURN NONE;
ALTER FIELD a ON src TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 100;
DEFINE TABLE v AS SELECT a FROM src;
DEFINE TABLE tot AS SELECT math::sum(a) AS s FROM src GROUP ALL;
SELECT * FROM v ORDER BY id;
SELECT * FROM tot;
UPDATE src:2 SET a = 1 RETURN NONE;
SELECT * FROM tot;
ALTER FIELD a ON src TYPE int MIGRATE EXISTING ROWS AS a_plus USING $value + 1 FALLBACK 0;
