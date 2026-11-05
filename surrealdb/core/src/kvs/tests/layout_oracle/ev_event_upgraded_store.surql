-- A store an earlier release wrote: an event defined and fired at the Legacy feature level reads
-- its rows as they were written. After the upgrade two migrations of the field stack, and an event
-- defined between them sees a row still behind both steps as the value both steps give, the same
-- as a read does, not the value of one of them. The job's rewrite of such a row fires no event.
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD n ON tb TYPE string;
DEFINE TABLE log SCHEMALESS;
DEFINE EVENT old ON tb WHEN $event = 'CREATE' THEN (CREATE log SET ev = 'old', n = $after.n);
CREATE tb:1 SET n = '1';
UPDATE tb:1 SET n = '2';
CREATE tb:2 SET n = '3';
CREATE tb:3 SET n = '5';
SELECT ev, n FROM log ORDER BY ev, n;
-- @upgrade
ALTER FIELD n ON tb TYPE int MIGRATE EXISTING ROWS AS m1 USING <int>$value + 100 FALLBACK 0;
DEFINE EVENT newer ON tb WHEN $event = 'UPDATE' THEN (CREATE log SET ev = 'newer', b = $before.n, a = $after.n);
UPDATE tb:1 SET n = 4;
UPDATE tb:2 SET n = 2;
ALTER FIELD n ON tb TYPE float MIGRATE EXISTING ROWS AS m2 USING <float>$value * 2f FALLBACK 0f;
DEFINE EVENT stacked ON tb WHEN $before.n > 150 OR $after.n > 150 THEN (CREATE log SET ev = 'stacked', b = $before.n, a = $after.n);
UPDATE tb:3 SET n = 1.5;
UPDATE tb:1 SET n = 0.5;
SELECT ev, n, b, a FROM log ORDER BY ev, n, b, a;
SELECT b, a FROM log WHERE ev = 'stacked';
SELECT * FROM tb ORDER BY id;
-- @drain
SELECT * FROM tb ORDER BY id;
SELECT ev, b, a FROM log WHERE ev = 'stacked';
SELECT count() AS c FROM log GROUP ALL;
-- @check
