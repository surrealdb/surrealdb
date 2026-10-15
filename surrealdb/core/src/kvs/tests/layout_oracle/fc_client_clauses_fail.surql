-- Clause changes in a transaction the caller owns, where statements fail partway
-- and the caller commits anyway: a statement that fails keeps what it wrote
-- before failing, and a later statement in the same transaction reads what the
-- earlier ones did. A clause that a refused statement would have added is not
-- in force for later statements of the transaction, nor for rows read after it.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE int;
DEFINE FIELD b ON p TYPE option<int>;
DEFINE FIELD s ON p TYPE string;
CREATE p:1 SET a = 1, s = '1' RETURN NONE;
CREATE p:2 SET a = 5, s = '2' RETURN NONE;
-- @upgrade
ALTER FIELD s ON p TYPE int MIGRATE EXISTING ROWS AS s_int USING <int>$value FALLBACK 0;
-- @drain
-- @client
ALTER FIELD a ON p ASSERT $value > 3;
ALTER FIELD b ON p DEFAULT ALWAYS 9 READONLY;
CREATE p:3 SET a = 1, s = 3 RETURN NONE;
UPDATE p:2 SET b = 4 RETURN NONE;
ALTER FIELD s ON p REFERENCE;
ALTER FIELD s ON p DEFAULT ALWAYS 1;
CREATE p:4 SET a = 9, s = 4 RETURN NONE;
UPDATE p:1 SET a = 2 RETURN NONE;
-- @end
SELECT * FROM p ORDER BY id;
(INFO FOR TABLE p).fields;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
-- Writes after the transaction see the clauses it left in force.
UPDATE p:2 SET b = 8 RETURN NONE;
UPDATE p:2 SET a = 6 RETURN NONE;
CREATE p:5 SET a = 7, s = 5 RETURN NONE;
SELECT * FROM p ORDER BY id;
-- @check
-- A transaction that drops what it added, then fails on the same field.
-- @client
ALTER FIELD a ON p DROP ASSERT;
ALTER FIELD b ON p DROP READONLY DROP DEFAULT;
ALTER FIELD s ON p DEFAULT ALWAYS 2 MIGRATE EXISTING ROWS AS s_int2 USING <int>$value FALLBACK 0;
ALTER FIELD a ON p TYPE string;
UPDATE p:1 SET a = 3 RETURN NONE;
-- @end
SELECT * FROM p ORDER BY id;
(INFO FOR TABLE p).fields;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.migrations;
-- @drain
SELECT * FROM p ORDER BY id;
-- @check
