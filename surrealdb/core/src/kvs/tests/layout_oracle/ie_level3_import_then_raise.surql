-- At compatibility level 3, an OPTION IMPORT write that misfits a migrated field is stored as written and
-- forfeits that field's guarantee. Raising the database to level 4 rewrites nothing, and the misfit row
-- reads as it was stored at level 4 as well. A write that does not fit is refused at both levels. The
-- import runs in a BEGIN block, since a bare OPTION statement reaches nothing after it.
DEFINE DATABASE d3 COMPATIBILITY 3;
USE DB d3;
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE string;
DEFINE FIELD b ON p TYPE string;
CREATE p:1 SET a = '1', b = '2' RETURN NONE;
ALTER FIELD a ON p TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
(INFO FOR TABLE p).migrations.guaranteed;
BEGIN;
OPTION IMPORT;
CREATE p:2 SET a = 'x', b = 7 RETURN NONE;
CREATE p:3 SET a = '5', b = '6' RETURN NONE;
OPTION IMPORT = false;
COMMIT;
'a' IN (INFO FOR TABLE p).migrations.guaranteed;
SELECT id, a, type::is_int(a) AS ai, b, type::is_int(b) AS bi FROM p ORDER BY id;
-- At level 3 an ordinary write to the misfit field is checked against it again.
CREATE p:4 SET a = 'y', b = 'q' RETURN NONE;
ALTER DATABASE COMPATIBILITY 4;
(INFO FOR DB).compatibility;
SELECT id, a, type::is_int(a) AS ai, b FROM p ORDER BY id;
-- @check
-- A migration of the other field after the raise reaches the misfit row in it.
ALTER FIELD b ON p TYPE int MIGRATE EXISTING ROWS AS b_int USING <int>$value FALLBACK 0;
SELECT id, a, b, type::is_int(b) AS bi FROM p ORDER BY id;
-- @drain
SELECT id, a, b FROM p ORDER BY id;
(INFO FOR TABLE p).migrations.guaranteed;
-- @check
-- A level-4 write after the raise is coerced; a write that does not fit is refused, and the misfit rows
-- are not touched by either.
CREATE p:5 SET a = 9, b = 8 RETURN NONE;
CREATE p:6 SET a = 'z', b = 1 RETURN NONE;
SELECT id, a, b FROM p ORDER BY id;
-- @check
