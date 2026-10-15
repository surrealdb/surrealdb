-- An ASSERT that reads $before, on a field whose rows are behind a pending step. The
-- $before a write checks is the migrated value the row held: a write that does not raise
-- it is refused, one that does is accepted, a row created after the step sees NONE, and
-- the migration job's own rewrite of a pending row must not trip the assert.
DEFINE TABLE m SCHEMAFULL;
DEFINE FIELD n ON m TYPE string;
DEFINE FIELD other ON m TYPE option<int>;
CREATE m:1 SET n = '1';
CREATE m:2 SET n = '2';
CREATE m:3 SET n = '3';
CREATE m:5 SET n = '5';
-- @upgrade
ALTER FIELD n ON m TYPE int MIGRATE EXISTING ROWS AS n_plus USING <int>$value + 10 FALLBACK -1;
ALTER FIELD n ON m TYPE int ASSERT $before = NONE OR $value > $before;
SELECT VALUE [record::id(id), n] FROM m ORDER BY id;
UPDATE m:1 SET n = 12 RETURN BEFORE;
UPDATE m:2 SET n = 12;
UPDATE m:3 SET n += 1 RETURN AFTER;
UPDATE m:3 SET n = 13;
UPDATE m:2 SET other = 1;
CREATE m:4 SET n = 4;
UPDATE m:4 SET n = 3;
UPDATE m:4 SET n = 5 RETURN AFTER;
-- @check
SELECT VALUE [record::id(id), n] FROM m ORDER BY id;
-- @drain
SELECT VALUE [record::id(id), n, other] FROM m ORDER BY id;
