-- Id ranges over a table whose keys are numbers, a string that looks like a number, a word and a uuid, with
-- a migrated field behind a step: a range answers only the keys its bounds admit, a single-key read answers
-- the migrated value, and an UPDATE or a DELETE by range writes exactly the rows it names.
DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD id ON t TYPE int | string | uuid;
DEFINE FIELD n ON t TYPE string;
DEFINE FIELD tag ON t TYPE option<string>;
CREATE t:1 SET n = '1' RETURN NONE;
CREATE t:2 SET n = '2' RETURN NONE;
CREATE t:3 SET n = '3' RETURN NONE;
CREATE t:4 SET n = '4' RETURN NONE;
CREATE t:5 SET n = '5' RETURN NONE;
CREATE t:⟨5⟩ SET n = '50' RETURN NONE;
CREATE t:abc SET n = '70' RETURN NONE;
CREATE t:u'018f9a3c-0000-7000-8000-000000000004' SET n = '80' RETURN NONE;
-- @upgrade
ALTER FIELD n ON t TYPE int MIGRATE EXISTING ROWS AS n_x10 USING (<int>$value) * 10 FALLBACK -1;
SELECT VALUE n FROM t:2..5 ORDER BY id;
SELECT VALUE n FROM t:2..=5 ORDER BY id;
SELECT VALUE n FROM t:..=2 ORDER BY id;
SELECT VALUE n FROM t:4.. ORDER BY id;
SELECT * FROM ONLY t:2;
(t:4).n;
t:3.n;
SELECT VALUE n FROM ONLY t:⟨5⟩;
-- @check
UPDATE t:1..3 SET tag = 'lo' RETURN AFTER;
SELECT VALUE n FROM t ORDER BY id;
DELETE t:4..=5 RETURN BEFORE;
SELECT VALUE id FROM t ORDER BY id;
-- @drain
SELECT * FROM t ORDER BY id;
-- @check
UPDATE t:2..=3 SET n = 6 RETURN AFTER;
SELECT VALUE n FROM t ORDER BY id;
UPDATE t:..=2 SET tag = 'head' RETURN AFTER;
SELECT VALUE [id, n, tag] FROM t ORDER BY id;
-- @check
