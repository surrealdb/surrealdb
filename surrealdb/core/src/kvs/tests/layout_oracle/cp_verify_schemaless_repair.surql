-- A SCHEMALESS table at compatibility level 4 takes a TYPE change with no MIGRATE over rows that break it. The
-- rows stay as stored, the verification counts the rows that break the new TYPE, a repair by a write moves the
-- count down, a write that the new TYPE refuses leaves its row alone, and no guarantee is claimed for the field
-- until every row conforms. An ASSERT added to another field afterwards is verified over the rows the same way.
DEFINE TABLE sv SCHEMALESS;
DEFINE FIELD n ON sv TYPE option<string>;
DEFINE FIELD m ON sv TYPE option<int>;
CREATE sv:1 SET n = '1', m = 1 RETURN NONE;
CREATE sv:2 SET n = 'two', m = 2 RETURN NONE;
CREATE sv:3 SET n = 'x' RETURN NONE;
CREATE sv:4 SET n = '4', m = -4 RETURN NONE;
CREATE sv:5 SET m = 5 RETURN NONE;
CREATE sv:6 SET n = 'six' RETURN NONE;
-- @upgrade
ALTER DATABASE test COMPATIBILITY 4;
ALTER FIELD n ON sv TYPE option<int>;
(INFO FOR TABLE sv).migrations;
SELECT * FROM sv ORDER BY id;
-- @check
-- @drain
(INFO FOR TABLE sv).migrations;
SELECT * FROM sv ORDER BY id;
-- Repairs by writes. A write the new TYPE refuses leaves its row as it was.
UPDATE sv:2 SET n = 'two again' RETURN NONE;
UPDATE sv:1 SET n = 1 RETURN NONE;
UPDATE sv:3 SET n = 3 RETURN NONE;
(INFO FOR TABLE sv).migrations.verification;
(INFO FOR TABLE sv).migrations.guaranteed;
SELECT * FROM sv ORDER BY id;
-- @check
-- A row created now is checked against the new TYPE.
CREATE sv:7 SET n = 'seven' RETURN NONE;
CREATE sv:8 SET n = 8, m = 8 RETURN NONE;
-- @drain
(INFO FOR TABLE sv).migrations.verification;
(INFO FOR TABLE sv).migrations.guaranteed;
UPDATE sv:2 SET n = 2 RETURN NONE;
UPDATE sv:4 SET n = 4 RETURN NONE;
UPDATE sv:6 SET n = 6 RETURN NONE;
(INFO FOR TABLE sv).migrations.verification;
(INFO FOR TABLE sv).migrations.guaranteed;
-- @check
-- @drain
(INFO FOR TABLE sv).migrations.verification;
(INFO FOR TABLE sv).migrations.guaranteed;
SELECT * FROM sv ORDER BY id;
-- An ASSERT added to m over rows, one of which holds -4.
ALTER FIELD m ON sv ASSERT $value >= 0;
(INFO FOR TABLE sv).migrations.verification;
-- @drain
(INFO FOR TABLE sv).migrations.verification;
UPDATE sv:4 SET m = 4 RETURN NONE;
(INFO FOR TABLE sv).migrations.verification;
SELECT * FROM sv ORDER BY id;
-- @check
-- The same TYPE written again, with every row now conforming: does the verification run afresh?
ALTER FIELD n ON sv TYPE option<int>;
(INFO FOR TABLE sv).migrations.verification;
(INFO FOR TABLE sv).migrations.guaranteed;
-- @drain
(INFO FOR TABLE sv).migrations.verification;
(INFO FOR TABLE sv).migrations.guaranteed;
-- A compatibility lowering and raise verify the field afresh.
ALTER DATABASE test COMPATIBILITY 3;
ALTER DATABASE test COMPATIBILITY 4;
(INFO FOR TABLE sv).migrations.verification;
(INFO FOR TABLE sv).migrations.guaranteed;
-- @drain
(INFO FOR TABLE sv).migrations.verification;
(INFO FOR TABLE sv).migrations.guaranteed;
-- @check
