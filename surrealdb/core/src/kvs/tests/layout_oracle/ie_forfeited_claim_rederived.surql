-- A row an OPTION IMPORT writes that misfits a migrated field forfeits that field's guarantee, and
-- the other field keeps its own. A redefinition that drops the migration is refused while the row
-- misfits. A later migration of the field reaches the misfit row as it reaches any row behind a
-- step, and the guarantee holds again afterwards. Imports run in BEGIN blocks.
DEFINE TABLE q SCHEMAFULL;
DEFINE FIELD n ON q TYPE string;
DEFINE FIELD m ON q TYPE string;
CREATE q:1 SET n = '1', m = 'a' RETURN NONE;
CREATE q:2 SET n = '2', m = 'b' RETURN NONE;
ALTER FIELD n ON q TYPE int MIGRATE EXISTING ROWS AS n_int USING <int>$value FALLBACK 0;
ALTER FIELD m ON q TYPE int MIGRATE EXISTING ROWS AS m_int USING <int>$value FALLBACK 0;
(INFO FOR TABLE q).migrations.guaranteed;
-- @drain
(INFO FOR TABLE q).migrations.guaranteed;
BEGIN;
OPTION IMPORT;
CREATE q:3 SET n = 'x', m = 3 RETURN NONE;
CREATE q:4 SET n = 4, m = 4 RETURN NONE;
OPTION IMPORT = false;
COMMIT;
(INFO FOR TABLE q).migrations.guaranteed;
SELECT id, n, m, type::is_int(n) AS ni, type::is_int(m) AS mi FROM q ORDER BY id;
-- The field that misfits no longer guarantees its type, so a write is coerced again.
CREATE q:5 SET n = 'y', m = 5 RETURN NONE;
-- Dropping the migration while q:3 misfits is refused, or restates the type and leaves the table as it was.
ALTER FIELD n ON q TYPE int;
(INFO FOR TABLE q).migrations.guaranteed;
SELECT id, n FROM q ORDER BY id;
-- @check
-- A migration of the field under a new name mints a step that the misfit row is behind.
ALTER FIELD n ON q MIGRATE EXISTING ROWS AS n_again USING <int>$value FALLBACK -9;
(INFO FOR TABLE q).migrations.guaranteed;
SELECT id, n, type::is_int(n) AS ni FROM q ORDER BY id;
-- @drain
SELECT id, n FROM q ORDER BY id;
(INFO FOR TABLE q).migrations.guaranteed;
-- @check
-- The field holds the type the guarantee claims, so an ordinary write is checked against it again.
CREATE q:6 SET n = 'z', m = 6 RETURN NONE;
CREATE q:7 SET n = 7, m = 7 RETURN NONE;
SELECT id, n FROM q ORDER BY id;
-- A second import misfits the other field only, and the first field stays guaranteed.
BEGIN;
OPTION IMPORT;
CREATE q:8 SET n = 8, m = 'w' RETURN NONE;
OPTION IMPORT = false;
COMMIT;
(INFO FOR TABLE q).migrations.guaranteed;
SELECT id, n, m FROM q ORDER BY id;
-- @check
