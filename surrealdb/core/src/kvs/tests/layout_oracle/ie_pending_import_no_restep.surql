-- An OPTION IMPORT write into a table with pending steps stores the values it was given. No step
-- that a row written before the import is still behind is applied to the import's rows: a step that
-- doubles its value doubles only the rows written before it. A row an import writes in the
-- pre-migration shape reads as written, and a row it writes with an absent field stays absent.
-- The import runs in a BEGIN block, since a bare OPTION statement reaches nothing after it.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE string;
DEFINE FIELD b ON p TYPE string;
CREATE p:1 SET a = '1', b = '2' RETURN NONE;
CREATE p:2 SET a = '3', b = '4' RETURN NONE;
ALTER FIELD b ON p TYPE int MIGRATE EXISTING ROWS AS b_int USING <int>$value FALLBACK 0;
ALTER FIELD b ON p MIGRATE EXISTING ROWS AS b_double USING $value * 2 FALLBACK 0;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
SELECT id, b FROM p ORDER BY id;
BEGIN;
OPTION IMPORT;
CREATE p:3 SET a = '5', b = 5 RETURN NONE;
CREATE p:4 SET a = '7' RETURN NONE;
CREATE p:6 SET a = '8', b = '3' RETURN NONE;
OPTION IMPORT = false;
COMMIT;
SELECT id, a, b, type::is_int(b) AS bi FROM p ORDER BY id;
(INFO FOR TABLE p).migrations.guaranteed;
-- A write after the import is coerced again, and stamped at the table's current generation.
CREATE p:5 SET a = '9', b = 6 RETURN NONE;
SELECT id, b FROM p ORDER BY id;
-- @check
-- @drain
SELECT id, a, b FROM p ORDER BY id;
(INFO FOR TABLE p).migrations.guaranteed;
-- An update of a row the import left behind reads it through the steps once, not twice.
UPDATE p:1 SET a = 'x' RETURN NONE;
UPDATE p:3 SET a = 'y' RETURN NONE;
SELECT id, a, b FROM p ORDER BY id;
-- @check
-- An index defined after the import sees the values as the rows hold them.
DEFINE INDEX by_b ON p FIELDS b;
SELECT VALUE id FROM p WHERE b = 5 ORDER BY id;
SELECT VALUE id FROM p WHERE b = 10 ORDER BY id;
SELECT VALUE id FROM p WHERE b = 3 ORDER BY id;
-- @check
