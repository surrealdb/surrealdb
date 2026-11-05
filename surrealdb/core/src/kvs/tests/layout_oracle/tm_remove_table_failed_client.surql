-- A transaction its caller owns removes a table with rows queued for a migration, defines the table
-- again with another field type, and fails partway: the caller commits what the statements before
-- the failure wrote, the removed table's queued work reaches none of the rows the new one holds, and
-- a migration minted and removed in one transaction leaves no step behind.
DEFINE TABLE rf SCHEMAFULL;
DEFINE FIELD a ON rf TYPE string;
CREATE rf:1 SET a = '5' RETURN NONE;
CREATE rf:2 SET a = 'x' RETURN NONE;
CREATE rf:3 SET a = '9' RETURN NONE;
-- @upgrade
ALTER FIELD a ON rf TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value + 1000 FALLBACK -1;
SELECT * FROM rf ORDER BY id;
-- @check
-- @client
REMOVE TABLE rf;
DEFINE TABLE rf SCHEMAFULL;
DEFINE FIELD a ON rf TYPE int;
CREATE rf:1 SET a = 1 RETURN NONE;
CREATE rf:2 SET a = 'boom' RETURN NONE;
CREATE rf:5 SET a = 5 RETURN NONE;
-- @end
SELECT * FROM rf ORDER BY id;
(INFO FOR TABLE rf).migrations;
SELECT VALUE type::is_int(a) FROM rf ORDER BY id;
-- @drain
SELECT * FROM rf ORDER BY id;
ALTER FIELD a ON rf TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value * 3 FALLBACK -1;
SELECT * FROM rf ORDER BY id;
-- @check
-- A migration minted and its table removed in the same transaction: nothing of it remains.
-- @client
ALTER FIELD a ON rf TYPE float MIGRATE EXISTING ROWS AS a_float USING <float>$value FALLBACK 0.0;
REMOVE TABLE rf;
CREATE rf:1 SET a = 2 RETURN NONE;
-- @end
SELECT * FROM rf ORDER BY id;
(INFO FOR TABLE rf).migrations;
-- @drain
SELECT * FROM rf ORDER BY id;
-- @check
