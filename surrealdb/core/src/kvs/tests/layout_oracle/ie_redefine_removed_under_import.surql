-- A field removed while rows are behind its steps loses its value in those rows, and an OPTION IMPORT
-- that defines a field of the same name afterwards does not bring the removed value back. The import's
-- own rows keep the values they were written with, and a migration over a field reaches a row with no
-- value for it the way it reaches any row behind a step. The import runs in a BEGIN block.
DEFINE TABLE r SCHEMAFULL;
DEFINE FIELD k ON r TYPE string;
DEFINE FIELD v ON r TYPE string;
DEFINE FIELD w ON r TYPE option<string>;
CREATE r:1 SET k = 'a', v = '1' RETURN NONE;
CREATE r:2 SET k = 'b', v = '2', w = 'two' RETURN NONE;
CREATE r:9 SET k = 'z', w = 'nine' RETURN NONE;
ALTER FIELD v ON r TYPE int MIGRATE EXISTING ROWS AS v_int USING <int>$value FALLBACK 0;
REMOVE FIELD v ON r;
(INFO FOR TABLE r).migrations.generation;
SELECT * FROM r ORDER BY id;
BEGIN;
OPTION IMPORT;
DEFINE FIELD v ON r TYPE option<int>;
CREATE r:3 SET k = 'c', v = 7 RETURN NONE;
CREATE r:4 SET k = 'd', v = 'four' RETURN NONE;
OPTION IMPORT = false;
COMMIT;
SELECT * FROM r ORDER BY id;
SELECT VALUE id FROM r WHERE v = 7 ORDER BY id;
(INFO FOR TABLE r).migrations.generation;
(INFO FOR TABLE r).migrations.guaranteed;
-- @check
-- @drain
SELECT * FROM r ORDER BY id;
-- A write after the import is coerced to the field's type.
CREATE r:5 SET k = 'e', v = 9 RETURN NONE;
CREATE r:6 SET k = 'f', v = 'nine' RETURN NONE;
SELECT * FROM r ORDER BY id;
-- @check
-- A migration over the field reaches the rows that have no value for it.
ALTER FIELD v ON r TYPE option<int> MIGRATE EXISTING ROWS AS v_plus USING $value + 100 FALLBACK 0;
SELECT * FROM r ORDER BY id;
(INFO FOR TABLE r).migrations.guaranteed;
-- @check
-- @drain
SELECT * FROM r ORDER BY id;
-- @check
