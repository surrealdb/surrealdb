-- A table made SCHEMALESS and made SCHEMAFULL again, with a field guaranteed and a COMPUTED field over it. While
-- the table is SCHEMALESS a declared field's TYPE is still checked on write. A retype made while SCHEMALESS, with
-- no MIGRATE, leaves the rows as stored and forfeits the old guarantee; making the table SCHEMAFULL again judges
-- the rows against the fields as they are now, and each claim is what every row holds.
DEFINE TABLE ft SCHEMAFULL;
DEFINE FIELD a ON ft TYPE string;
DEFINE FIELD b ON ft TYPE option<string>;
DEFINE FIELD c ON ft COMPUTED a ?? 'none';
CREATE ft:1 SET a = '1', b = 'x' RETURN NONE;
CREATE ft:2 SET a = '2' RETURN NONE;
-- @upgrade
ALTER FIELD a ON ft TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
-- @drain
(INFO FOR TABLE ft).migrations.guaranteed;
SELECT * FROM ft ORDER BY id;
-- @check
DEFINE TABLE OVERWRITE ft SCHEMALESS;
(INFO FOR TABLE ft).migrations.guaranteed;
CREATE ft:3 SET a = 'three', b = 'y' RETURN NONE;
CREATE ft:3 SET a = 3, b = 'y' RETURN NONE;
CREATE ft:4 SET b = 'no a' RETURN NONE;
UPDATE ft:1 SET a = 'back' RETURN NONE;
SELECT * FROM ft ORDER BY id;
-- @check
-- A retype made while SCHEMALESS, with no MIGRATE: the rows stay as stored and the old guarantee goes.
ALTER FIELD a ON ft TYPE string;
(INFO FOR TABLE ft).migrations.guaranteed;
SELECT * FROM ft ORDER BY id;
UPDATE ft:3 SET b = 'three' RETURN NONE;
-- @check
DEFINE TABLE OVERWRITE ft SCHEMAFULL;
(INFO FOR TABLE ft).migrations;
SELECT * FROM ft ORDER BY id;
UPDATE ft:1 SET a = 'one' RETURN NONE;
-- @check
-- @drain
(INFO FOR TABLE ft).migrations;
SELECT * FROM ft ORDER BY id;
-- @check
