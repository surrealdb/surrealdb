/**
[test]
reason = "An event reads rows behind a migration step as migrated: `$before` and `$after` hold the migrated values of every field the step reaches, an event defined before the migration as well as one defined after it, for an update, a delete and a create alike, and a WHEN condition judges the migrated value."

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
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ af: { a: 4, id: tb:4 }, b: NONE, ev: 'CREATE', src: 'after' }, { af: NONE, b: { a: -1, id: tb:2 }, ev: 'DELETE', src: 'after' }, { af: { a: 6, id: tb:1, o: { n: 1 } }, b: { a: 5, id: tb:1, o: { n: 1 } }, ev: 'UPDATE', src: 'after' }, { af: { a: 9, id: tb:3, o: { n: 7 } }, b: { a: 9, id: tb:3 }, ev: 'UPDATE', src: 'after' }, { af: { a: 4, id: tb:4 }, b: NONE, ev: 'CREATE', src: 'before' }, { af: NONE, b: { a: -1, id: tb:2 }, ev: 'DELETE', src: 'before' }, { af: { a: 6, id: tb:1, o: { n: 1 } }, b: { a: 5, id: tb:1, o: { n: 1 } }, ev: 'UPDATE', src: 'before' }, { af: { a: 9, id: tb:3, o: { n: 7 } }, b: { a: 9, id: tb:3 }, ev: 'UPDATE', src: 'before' }]"

[[test.results]]
value = "[{ now: 6, row: tb:1, was: 5 }]"

[[test.results]]
value = "[true]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ now: 6, row: tb:1, was: 5 }, { now: 8, row: tb:4, was: 5 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'lvl3', namespace: 'test' }"

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
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ af: NONE, b: { a: -1, id: tb:2 }, ev: 'DELETE' }, { af: { a: 6, id: tb:1 }, b: { a: 5, id: tb:1 }, ev: 'UPDATE' }]"
*/
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
DEFINE FIELD o ON tb TYPE option<object>;
DEFINE FIELD o.n ON tb TYPE string;
DEFINE TABLE log SCHEMALESS;
DEFINE TABLE flagged SCHEMALESS;
CREATE tb:1 SET a = '5', o = { n: '1' } RETURN NONE;
CREATE tb:2 SET a = 'x' RETURN NONE;
CREATE tb:3 SET a = '9' RETURN NONE;
-- Defined before the migration.
DEFINE EVENT ev_before ON tb WHEN true THEN (CREATE log SET src = 'before', ev = $event, b = $before, af = $after);
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
ALTER FIELD o.n ON tb TYPE int MIGRATE EXISTING ROWS AS on_int USING <int>$value FALLBACK -1;
-- Defined after it.
DEFINE EVENT ev_after ON tb WHEN true THEN (CREATE log SET src = 'after', ev = $event, b = $before, af = $after);
DEFINE EVENT ev_five ON tb WHEN $before.a = 5 THEN (CREATE flagged SET row = $before.id, was = $before.a, now = $after.a);
UPDATE tb:1 SET a = 6 RETURN NONE;
DELETE tb:2;
CREATE tb:4 SET a = 4 RETURN NONE;
UPDATE tb:3 SET o = { n: 7 } RETURN NONE;
SELECT src, ev, b, af FROM log ORDER BY src, ev, b.id;
SELECT row, was, now FROM flagged;
SELECT VALUE type::is_int(was) FROM flagged;
-- A row written after the step has nothing to migrate and is judged as it was written.
UPDATE tb:4 SET a = 5 RETURN NONE;
UPDATE tb:4 SET a = 8 RETURN NONE;
SELECT row, was, now FROM flagged ORDER BY row;
-- The same at compatibility level 3.
DEFINE DATABASE lvl3 COMPATIBILITY 3;
USE DB lvl3;
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
DEFINE TABLE log SCHEMALESS;
CREATE tb:1 SET a = '5' RETURN NONE;
CREATE tb:2 SET a = 'junk' RETURN NONE;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
DEFINE EVENT ev ON tb WHEN true THEN (CREATE log SET ev = $event, b = $before, af = $after);
UPDATE tb:1 SET a = 6 RETURN NONE;
DELETE tb:2;
SELECT ev, b, af FROM log ORDER BY ev;
