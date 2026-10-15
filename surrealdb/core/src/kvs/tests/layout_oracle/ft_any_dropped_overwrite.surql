-- A concrete TYPE dropped through DEFINE FIELD OVERWRITE (the field has no TYPE
-- and accepts every value), rows of mixed types written while it has none, and
-- the TYPE restored to int through a MIGRATE that must cover those rows. Also a
-- TYPE any field narrowed to int, and a caller-owned transaction in which a
-- retype is followed by a failing write: the retype keeps, the failed write does not.
DEFINE TABLE dy SCHEMAFULL;
DEFINE FIELD n ON dy TYPE int;
DEFINE FIELD m ON dy TYPE any;
CREATE dy:1 SET n = 1, m = 'x';
CREATE dy:2 SET n = 2, m = 2;
CREATE dy:3 SET n = 3, m = NULL;
CREATE dy:4 SET n = 4;
-- @upgrade
DEFINE FIELD OVERWRITE n ON dy;
CREATE dy:5 SET n = 'five', m = [1, 2];
CREATE dy:6 SET n = { k: 6 }, m = { k: 6 };
SELECT VALUE [n, m] FROM dy ORDER BY id;
(INFO FOR TABLE dy).fields;
(INFO FOR TABLE dy).migrations.generation;
-- @check
-- Restoring int over rows holding a string, an object and ints. Refused unless
-- the migration covers them.
DEFINE FIELD OVERWRITE n ON dy TYPE int;
DEFINE FIELD OVERWRITE n ON dy TYPE int MIGRATE EXISTING ROWS AS n_int USING <int>$value FALLBACK -1;
SELECT VALUE [n, m] FROM dy ORDER BY id;
(INFO FOR TABLE dy).migrations.generation;
-- @drain
SELECT VALUE [n, m] FROM dy ORDER BY id;
-- The any field narrowed to int: the object and array rows take the FALLBACK.
ALTER FIELD m ON dy TYPE int MIGRATE EXISTING ROWS AS m_int USING <int>$value;
ALTER FIELD m ON dy TYPE int MIGRATE EXISTING ROWS AS m_int USING <int>$value FALLBACK -1;
SELECT VALUE [n, m] FROM dy ORDER BY id;
SELECT VALUE id FROM dy WHERE m = -1 ORDER BY id;
SELECT VALUE m FROM ONLY dy:4;
SELECT VALUE m FROM ONLY dy:3;
-- @check
-- A caller's transaction: a retype, then a write that fails, then a retype that
-- is refused. The caller commits; only the first change and no part of the
-- failed write may show.
-- @client
DEFINE FIELD OVERWRITE m ON dy TYPE option<int> MIGRATE EXISTING ROWS AS m_opt USING $value FALLBACK NONE;
CREATE dy:7 SET n = 7, m = 'bad-int-check';
DEFINE FIELD OVERWRITE n ON dy TYPE string;
-- @end
SELECT VALUE [n, m] FROM dy ORDER BY id;
(INFO FOR TABLE dy).fields;
(INFO FOR TABLE dy).migrations.generation;
-- @check
-- Rows written after the restored TYPE are held to it.
CREATE dy:8 SET n = 8, m = 8;
CREATE dy:9 SET n = 'nine', m = 9;
SELECT VALUE [n, m] FROM dy ORDER BY id;
-- @drain
SELECT VALUE [n, m] FROM dy ORDER BY id;
(INFO FOR TABLE dy).migrations.generation;
-- @check
-- The index on m is defined last: the index on a field blocks a migration of it.
DEFINE INDEX by_m ON dy FIELDS m;
SELECT VALUE id FROM dy WHERE m = -1 ORDER BY id;
-- @check
