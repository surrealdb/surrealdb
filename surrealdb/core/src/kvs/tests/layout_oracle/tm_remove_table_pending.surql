-- A table removed while its rows are still queued for a migration: the queued work reaches no
-- row the table defined under the same name afterwards holds, a row of the removed table whose id
-- the new table does not reuse is not read again, and the new table's migration reuses the removed
-- table's field and migration names with another transform.
DEFINE TABLE rmq SCHEMAFULL;
DEFINE FIELD a ON rmq TYPE string;
CREATE rmq:1 SET a = '5' RETURN NONE;
CREATE rmq:2 SET a = '6' RETURN NONE;
CREATE rmq:3 SET a = 'x' RETURN NONE;
CREATE rmq:4 SET a = '8' RETURN NONE;
-- @upgrade
ALTER FIELD a ON rmq TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value + 1000 FALLBACK -1;
SELECT * FROM rmq ORDER BY id;
(INFO FOR TABLE rmq).migrations.generation;
-- @check
-- Removed while the job has not yet reached its rows.
REMOVE TABLE rmq;
SELECT * FROM rmq ORDER BY id;
DEFINE TABLE rmq SCHEMAFULL;
DEFINE FIELD a ON rmq TYPE string;
CREATE rmq:1 SET a = '5' RETURN NONE;
CREATE rmq:2 SET a = '6' RETURN NONE;
SELECT * FROM rmq ORDER BY id;
SELECT * FROM rmq:3;
SELECT * FROM rmq:4;
(INFO FOR TABLE rmq).migrations;
-- @drain
SELECT * FROM rmq ORDER BY id;
(INFO FOR TABLE rmq).migrations;
-- The same field and migration names, another transform: the step is the new table's alone.
ALTER FIELD a ON rmq TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value * 2 FALLBACK -1;
SELECT * FROM rmq ORDER BY id;
SELECT VALUE a FROM rmq ORDER BY id;
(INFO FOR TABLE rmq).migrations.migrations;
-- @drain
SELECT * FROM rmq ORDER BY id;
SELECT VALUE type::is_int(a) FROM rmq ORDER BY id;
-- A row written after the second migration is read at the generation it was written at.
CREATE rmq:5 SET a = '7' RETURN NONE;
SELECT * FROM rmq ORDER BY id;
-- @check
-- Removed again with a queued migration: nothing of the second table reaches a third one.
ALTER FIELD a ON rmq TYPE int MIGRATE EXISTING ROWS AS a_int2 USING <int>$value - 1 FALLBACK 0;
REMOVE TABLE rmq;
DEFINE TABLE rmq SCHEMAFULL;
DEFINE FIELD a ON rmq TYPE string;
SELECT * FROM rmq ORDER BY id;
CREATE rmq:1 SET a = '1' RETURN NONE;
SELECT * FROM rmq ORDER BY id;
(INFO FOR TABLE rmq).migrations;
-- @drain
SELECT * FROM rmq ORDER BY id;
-- @check
