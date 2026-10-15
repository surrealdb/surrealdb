-- A view over a migrated table and an index on the migrated field, with OPTION IMPORT writes made after
-- both exist. The index must answer what the rows hold, and every read of the table must agree with it.
-- OPTION IMPORT does not maintain views, so this probes what the view answers after an import and what
-- an export of the database carries. The import runs in a BEGIN block.
DEFINE TABLE src SCHEMAFULL;
DEFINE FIELD n ON src TYPE string;
DEFINE FIELD k ON src TYPE string;
CREATE src:1 SET n = '1', k = 'a' RETURN NONE;
CREATE src:2 SET n = '2', k = 'b' RETURN NONE;
ALTER FIELD n ON src TYPE int MIGRATE EXISTING ROWS AS n_int USING <int>$value FALLBACK 0;
DEFINE INDEX by_n ON src FIELDS n;
DEFINE TABLE vw AS SELECT n, k FROM src WHERE n > 1;
SELECT * FROM vw ORDER BY id;
SELECT VALUE id FROM src WHERE n > 1 ORDER BY id;
-- @check
BEGIN;
OPTION IMPORT;
CREATE src:3 SET n = 5, k = 'c' RETURN NONE;
CREATE src:4 SET n = 'x', k = 'd' RETURN NONE;
OPTION IMPORT = false;
COMMIT;
SELECT * FROM vw ORDER BY id;
SELECT VALUE id FROM src WHERE n > 1 ORDER BY id;
SELECT VALUE id FROM src WHERE n = 5 ORDER BY id;
SELECT VALUE id FROM src WHERE n = 'x' ORDER BY id;
-- @check
-- A non-import write is maintained through the view and the index.
CREATE src:5 SET n = 9, k = 'e' RETURN NONE;
SELECT * FROM vw ORDER BY id;
UPDATE src:1 SET n = 4 RETURN NONE;
SELECT * FROM vw ORDER BY id;
-- @check
-- @drain
SELECT * FROM vw ORDER BY id;
SELECT VALUE id FROM src WHERE n > 1 ORDER BY id;
-- @check
