/**
[env]
timeout = 10000

[test]
reason = "A view row takes the id of its source row unless the view's projection names the row's `id`, so a view over two views derived from the same table holds one row per source id by default, and one row per source row when it projects an `id` that tells them apart. A projected `id` keys the row when the view is defined and on every later write: the row moves when its `id` changes and goes when its record leaves the view, and an `id` that names no one key — `NONE` or a range — fails the write. An unaliased path into `id` (`id.x`) sets the row's `id` as an alias does."
issue = 2561

[[test.results]]
value = "[{ id: a:1, x: 1, y: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: d:1, z: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: e:['b', 1], z: 1 }, { id: e:['c', 1], z: 2 }]"

[[test.results]]
value = "[{ id: a:2, x: 3, y: 4 }]"

[[test.results]]
value = "[{ id: e:['b', 1], z: 1 }, { id: e:['b', 2], z: 3 }, { id: e:['c', 1], z: 2 }, { id: e:['c', 2], z: 4 }]"

[[test.results]]
value = "[{ id: a:2, x: 5, y: 4 }]"

[[test.results]]
value = "[{ id: e:['b', 1], z: 1 }, { id: e:['b', 2], z: 5 }, { id: e:['c', 1], z: 2 }, { id: e:['c', 2], z: 4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: e:['b', 2], z: 5 }, { id: e:['c', 2], z: 4 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: f:50, x: 5 }]"

[[test.results]]
value = "[{ id: a:2, x: 6, y: 4 }]"

[[test.results]]
value = "[{ id: f:60, x: 6 }]"

[[test.results]]
value = "[{ id: a:2, x: -1, y: 4 }]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Found NONE for the Record ID but this is not a valid id"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found NONE for the Record ID but this is not a valid id"

[[test.results]]
value = "[{ id: h:-1, x: -1 }]"

[[test.results]]
error = "Found r:1..3 for the Record ID but this is not a valid id"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: p:{ x: -1 }, x: -1 }]"

[[test.results]]
value = "[{ id: a:3, x: 7 }]"

[[test.results]]
value = "[{ id: p:{ x: -1 }, x: -1 }, { id: p:{ x: 7 }, x: 7 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: a:4, x: 8 }]"

[[test.results]]
value = "[{ id: t:2, src: s:2 }, { id: t:3, src: s:3 }, { id: t:4, src: s:4 }]"

*/

CREATE a:1 SET x = 1, y = 2;
DEFINE TABLE b AS SELECT x AS z FROM a;
DEFINE TABLE c AS SELECT y AS z FROM a;
-- By default the row is keyed by its source row: b:1 and c:1 make one row.
DEFINE TABLE d AS SELECT * FROM b, c;
SELECT * FROM d;
-- A projected id keeps them apart, when the view is defined and on later writes.
DEFINE TABLE e AS SELECT *, type::record('e', [record::tb(id), record::id(id)]) AS id FROM b, c;
SELECT * FROM e;
CREATE a:2 SET x = 3, y = 4;
SELECT * FROM e;
UPDATE a:2 SET x = 5;
SELECT * FROM e;
DELETE a:1;
SELECT * FROM e;
-- A row whose projected id changes moves, and leaves the view with its record.
DEFINE TABLE f AS SELECT x, x * 10 AS id FROM a WHERE x > 0;
SELECT * FROM f;
UPDATE a:2 SET x = 6;
SELECT * FROM f;
UPDATE a:2 SET x = -1;
SELECT * FROM f;
-- An id that names no key fails the write, whether the view is defined over rows or written later.
DEFINE TABLE g AS SELECT x, NONE AS id FROM a;
DEFINE TABLE h AS SELECT x, IF x > 100 THEN NONE ELSE x END AS id FROM a;
UPDATE a:2 SET x = 200;
SELECT * FROM h;
-- A range names no one record.
DEFINE TABLE r AS SELECT x, type::record('r', 1..3) AS id FROM a;
-- An unaliased path into the record's id sets the row's id, when defined and on later writes.
DEFINE TABLE p AS SELECT x, id.x FROM a;
SELECT * FROM p;
CREATE a:3 SET x = 7;
SELECT * FROM p;
-- A view over a view reads the view row's own id, whether the row came from the backfill or a later write.
DEFINE TABLE s AS SELECT * FROM a;
DEFINE TABLE t AS SELECT id AS src FROM s;
CREATE a:4 SET x = 8;
SELECT * FROM t;
