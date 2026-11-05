-- Stacked migrations queued before one job run: a field's chain string to int
-- to float, and a second field of the same table on its own chain. A row written
-- between the two steps of a chain passes through the second step only, and a
-- row the first step left behind passes through both, in order, exactly once.
DEFINE TABLE m SCHEMAFULL;
DEFINE FIELD a ON m TYPE string;
DEFINE FIELD b ON m TYPE string;
CREATE m:1 SET a = '1', b = '10' RETURN NONE;
CREATE m:2 SET a = '2.5', b = 'x' RETURN NONE;
CREATE m:3 SET a = 'x', b = '3' RETURN NONE;
-- @upgrade
CREATE m:4 SET a = '4', b = '4' RETURN NONE;
ALTER FIELD a ON m TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
CREATE m:5 SET a = 5, b = '5' RETURN NONE;
ALTER FIELD a ON m TYPE float MIGRATE EXISTING ROWS AS a_float USING <float>$value * 2 FALLBACK 0.5;
CREATE m:6 SET a = 6f, b = '6' RETURN NONE;
ALTER FIELD b ON m TYPE int MIGRATE EXISTING ROWS AS b_int USING <int>$value FALLBACK 0;
SELECT * FROM m ORDER BY id;
SELECT VALUE a FROM m ORDER BY id;
SELECT VALUE b FROM m ORDER BY id;
SELECT VALUE type::is_float(a) FROM m ORDER BY id;
SELECT VALUE id FROM m WHERE a > 3 ORDER BY id;
(INFO FOR TABLE m).migrations;
-- @check
-- A second table with its own chain, queued in the same batch.
DEFINE TABLE n SCHEMAFULL;
DEFINE FIELD v ON n TYPE string;
CREATE n:1 SET v = '7' RETURN NONE;
ALTER FIELD v ON n TYPE int MIGRATE EXISTING ROWS AS v_int USING <int>$value FALLBACK 0;
ALTER FIELD v ON n TYPE float MIGRATE EXISTING ROWS AS v_float USING <float>$value + 0.5 FALLBACK 0.5;
SELECT * FROM n;
(INFO FOR TABLE n).migrations;
-- @drain
SELECT * FROM m ORDER BY id;
SELECT * FROM n;
(INFO FOR TABLE m).migrations;
(INFO FOR TABLE n).migrations;
-- @check
-- A row written after the job ran is migrated by nothing.
CREATE m:7 SET a = 7.25f, b = 7 RETURN NONE;
SELECT * FROM m ORDER BY id;
ALTER FIELD v ON n TYPE float MIGRATE EXISTING ROWS AS v_float_again USING <float>$value * 3 FALLBACK 0.5;
SELECT * FROM n ORDER BY id;
-- Restating the first step of m.a's chain after the chain has moved on: the name
-- has run, so the statement is a no-op, and the TYPE it names is not applied.
ALTER FIELD a ON m TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
(INFO FOR TABLE m).fields;
SELECT VALUE a FROM m ORDER BY id;
-- @drain
-- @check
