-- A database at level 3 removed while a table in it has rows queued for a migration, and defined
-- again under the same name at level 3: the table of the same name starts with no rows, no steps and
-- no queued work, and a field re-typed without MIGRATE at level 3 is judged by the raise to level 4
-- over the new table's rows alone.
DEFINE DATABASE tm_db COMPATIBILITY 3;
USE DB tm_db;
DEFINE TABLE dbt SCHEMAFULL;
DEFINE FIELD a ON dbt TYPE string;
CREATE dbt:1 SET a = '5' RETURN NONE;
CREATE dbt:2 SET a = 'x' RETURN NONE;
CREATE dbt:3 SET a = '7' RETURN NONE;
-- @upgrade
ALTER FIELD a ON dbt TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value + 100 FALLBACK -1;
SELECT * FROM dbt ORDER BY id;
(INFO FOR TABLE dbt).migrations.generation;
-- @check
USE DB test;
REMOVE DATABASE tm_db;
DEFINE DATABASE tm_db COMPATIBILITY 3;
USE DB tm_db;
SELECT * FROM dbt;
DEFINE TABLE dbt SCHEMAFULL;
DEFINE FIELD a ON dbt TYPE string;
CREATE dbt:1 SET a = 'one' RETURN NONE;
CREATE dbt:3 SET a = '7' RETURN NONE;
SELECT * FROM dbt ORDER BY id;
(INFO FOR DB).compatibility;
(INFO FOR TABLE dbt).migrations;
-- @drain
SELECT * FROM dbt ORDER BY id;
(INFO FOR TABLE dbt).migrations;
-- Re-typed without MIGRATE at level 3: accepted, and the rows stay as stored.
ALTER FIELD a ON dbt TYPE int;
SELECT * FROM dbt ORDER BY id;
(INFO FOR TABLE dbt).migrations;
-- @drain
-- The raise verifies the rows of the new table; each row that fails is reported, none rewritten.
ALTER DATABASE tm_db COMPATIBILITY 4;
(INFO FOR DB).compatibility;
SELECT * FROM dbt ORDER BY id;
(INFO FOR TABLE dbt).migrations;
-- A migration of the removed table's name, minted at level 4 over the new rows, with another transform.
ALTER FIELD a ON dbt TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value * 10 FALLBACK -1;
SELECT * FROM dbt ORDER BY id;
(INFO FOR TABLE dbt).migrations.migrations;
-- @drain
SELECT * FROM dbt ORDER BY id;
(INFO FOR TABLE dbt).migrations;
USE DB test;
-- @check
