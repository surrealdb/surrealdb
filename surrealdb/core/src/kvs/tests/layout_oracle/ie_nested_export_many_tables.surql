-- Several tables are left mid-migration at the end: nested object and array paths with steps behind
-- them, a removed field with its removal step behind it, an index built while its rows are behind a
-- step, and a schemaless table with a migrated field. OPTION IMPORT writes go into each of them, in
-- a BEGIN block, and the end-of-scenario export and import must answer the rows as they read.
DEFINE TABLE t1 SCHEMAFULL;
DEFINE FIELD k ON t1 TYPE string;
DEFINE FIELD o ON t1 TYPE option<object>;
DEFINE FIELD o.n ON t1 TYPE option<string>;
DEFINE FIELD t ON t1 TYPE option<array<string>>;
CREATE t1:1 SET k = 'a', o = { n: '1' }, t = ['1', '2'] RETURN NONE;
CREATE t1:2 SET k = 'b', o = { n: '2' }, t = ['3'] RETURN NONE;
CREATE t1:3 SET k = 'c', o = {}, t = [] RETURN NONE;
DEFINE TABLE t2 SCHEMAFULL;
DEFINE FIELD g ON t2 TYPE string;
DEFINE FIELD h ON t2 TYPE string;
DEFINE FIELD x ON t2 TYPE string;
CREATE t2:1 SET g = '10', h = 'p', x = 'q' RETURN NONE;
CREATE t2:2 SET g = '20', h = 'r', x = 's' RETURN NONE;
DEFINE TABLE t3 SCHEMALESS;
DEFINE FIELD c ON t3 TYPE string;
CREATE t3:1 SET c = '7' RETURN NONE;
ALTER FIELD o.n ON t1 TYPE option<int> MIGRATE EXISTING ROWS AS on_int USING <int>$value FALLBACK -1;
DEFINE FIELD OVERWRITE t ON t1 TYPE option<array<int>> MIGRATE EXISTING ROWS AS t_int USING array::map($value, |$x| <int>$x) FALLBACK NONE;
ALTER FIELD g ON t2 TYPE int MIGRATE EXISTING ROWS AS g_int USING <int>$value FALLBACK 0;
REMOVE FIELD x ON t2;
DEFINE INDEX by_g ON t2 FIELDS g;
ALTER FIELD c ON t3 TYPE int MIGRATE EXISTING ROWS AS c_int USING <int>$value FALLBACK 0;
(INFO FOR TABLE t1).migrations;
(INFO FOR TABLE t2).migrations;
(INFO FOR TABLE t3).migrations;
SELECT * FROM t1 ORDER BY id;
SELECT * FROM t2 ORDER BY id;
SELECT * FROM t3 ORDER BY id;
-- @check
-- The import writes rows into each table while the steps are still behind.
BEGIN;
OPTION IMPORT;
CREATE t1:4 SET k = 'd', o = { n: 'x' }, t = ['z'] RETURN NONE;
CREATE t1:5 SET k = 'e', o = { n: 5 }, t = [5, 6] RETURN NONE;
CREATE t2:3 SET g = 'g3', h = 'h3' RETURN NONE;
CREATE t3:2 SET c = 'seven' RETURN NONE;
OPTION IMPORT = false;
COMMIT;
SELECT * FROM t1 ORDER BY id;
SELECT * FROM t2 ORDER BY id;
SELECT * FROM t3 ORDER BY id;
SELECT id, o.n AS n, t FROM t1 ORDER BY id;
-- Non-import writes to rows that are behind their steps.
UPDATE t1:1 SET o.n = 8 RETURN NONE;
UPDATE t2:1 SET h = 'hh' RETURN NONE;
UPDATE t3:1 SET c = 8 RETURN NONE;
SELECT VALUE id FROM t2 WHERE g = 10 ORDER BY id;
SELECT VALUE id FROM t2 WHERE g = 20 ORDER BY id;
SELECT VALUE id FROM t1 WHERE o.n = 5 ORDER BY id;
-- @check
-- A later step on the nested path, behind the earlier ones, with no drain.
ALTER FIELD o.n ON t1 TYPE option<int> MIGRATE EXISTING ROWS AS on_plus USING $value + 1 FALLBACK 0;
(INFO FOR TABLE t1).migrations.generation;
SELECT * FROM t1 ORDER BY id;
SELECT id, o.n AS n, t FROM t1 ORDER BY id;
-- The tables end mid-migration: steps are still queued, and rows are behind them.
(INFO FOR TABLE t1).migrations;
(INFO FOR TABLE t2).migrations;
