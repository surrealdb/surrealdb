-- Events on a table with a migration pending. A WHEN or THEN reads a row behind its step as the
-- migrated value, as a read does. The job's rewrite of such a row fires no event: the log holds
-- only what the statements wrote, and the lazy and eager runs agree on it.
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD n ON tb TYPE string;
DEFINE FIELD tag ON tb TYPE option<string>;
DEFINE TABLE log SCHEMALESS;
CREATE tb:1 SET n = '1';
CREATE tb:2 SET n = '2';
CREATE tb:3 SET n = 'x';
-- @upgrade
DEFINE EVENT before_mig ON tb WHEN $event != 'DELETE' THEN (CREATE log SET src = 'before', ev = $event, n = $after.n);
ALTER FIELD n ON tb TYPE int MIGRATE EXISTING ROWS AS m USING <int>$value FALLBACK -1;
DEFINE EVENT after_mig ON tb WHEN $before.n > 1 OR $after.n > 1 THEN (CREATE log SET src = 'after', ev = $event, b = $before.n, a = $after.n);
UPDATE tb:1 SET tag = 'a';
UPDATE tb:2 SET tag = 'b';
DELETE tb:3;
CREATE tb:4 SET n = 9;
SELECT src, ev, n, b, a FROM log ORDER BY src, ev, n, b, a;
-- @drain
SELECT src, ev, n, b, a FROM log ORDER BY src, ev, n, b, a;
SELECT * FROM tb ORDER BY id;
UPDATE tb SET tag = 'all' WHERE n > 0;
SELECT src, ev, n, b, a FROM log ORDER BY src, ev, n, b, a;
SELECT * FROM tb ORDER BY id;
-- @check
