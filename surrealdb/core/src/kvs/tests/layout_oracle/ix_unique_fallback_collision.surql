-- Rows whose stored values differ fall back to one migrated value, so a UNIQUE index over the
-- migrated field cannot be defined while both rows stand. Once one is gone, the index holds the
-- migrated values: a write to a row behind the step that collides with another row's migrated value
-- is refused, an update of another field leaves a row's unique key where it was, and REBUILD INDEX
-- keeps the same keys.
DEFINE TABLE uq SCHEMAFULL;
DEFINE FIELD a ON uq TYPE string;
DEFINE FIELD b ON uq TYPE string;
CREATE uq:1 SET a = 'junk', b = 'p' RETURN NONE;
CREATE uq:2 SET a = 'bad', b = 'q' RETURN NONE;
CREATE uq:3 SET a = '7', b = 'r' RETURN NONE;
CREATE uq:4 SET a = '8', b = 's' RETURN NONE;
-- @upgrade
ALTER FIELD a ON uq TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
DEFINE INDEX ua ON uq FIELDS a UNIQUE;
(INFO FOR TABLE uq).indexes;
DEFINE INDEX ub ON uq FIELDS b UNIQUE;
DELETE uq:2;
DEFINE INDEX ua ON uq FIELDS a UNIQUE;
SELECT VALUE id FROM uq WHERE a = 0 ORDER BY id;
SELECT VALUE id FROM uq WHERE a = 0 ORDER BY id EXPLAIN;
SELECT VALUE id FROM uq WHERE a >= 7 ORDER BY id;
-- @check
CREATE uq:5 SET a = 0, b = 'k' RETURN NONE;
CREATE uq:5 SET a = 7, b = 'k' RETURN NONE;
UPDATE uq:3 SET b = 'z' RETURN NONE;
CREATE uq:6 SET a = 7, b = 'm' RETURN NONE;
UPDATE uq:4 SET a = 0 RETURN NONE;
UPDATE uq:4 SET a = 9 RETURN NONE;
SELECT VALUE id FROM uq WHERE a = 7 ORDER BY id;
SELECT VALUE id FROM uq WHERE a = 9 ORDER BY id;
SELECT VALUE id FROM uq WHERE b = 'z' ORDER BY id;
-- @drain
SELECT VALUE a FROM uq ORDER BY id;
REBUILD INDEX ua ON uq;
SELECT VALUE id FROM uq WHERE a = 0 ORDER BY id;
SELECT VALUE id FROM uq WHERE a = 7 ORDER BY id;
-- @check
-- A UNIQUE index defined in the transaction that migrates its field. The stored '1' and '01' differ
-- and both migrate to 1, so the definition must be refused, as it is when the migration is committed alone.
DEFINE FIELD d ON uq TYPE option<string>;
UPDATE uq:1 SET d = '1' RETURN NONE;
UPDATE uq:3 SET d = '01' RETURN NONE;
UPDATE uq:4 SET d = '2' RETURN NONE;
BEGIN;
ALTER FIELD d ON uq TYPE option<int> MIGRATE EXISTING ROWS AS d_int USING <int>$value FALLBACK NONE;
DEFINE INDEX ud ON uq FIELDS d UNIQUE;
COMMIT;
(INFO FOR TABLE uq).indexes;
SELECT VALUE id FROM uq WHERE d = 1 ORDER BY id;
SELECT VALUE id FROM uq WHERE d = 1 ORDER BY id EXPLAIN;
SELECT VALUE d FROM uq ORDER BY id;
-- @drain
SELECT VALUE id FROM uq WHERE d = 1 ORDER BY id;
-- @check
-- Whether a REBUILD of the UNIQUE index built inside the transaction can succeed over the duplicates.
REBUILD INDEX ud ON uq;
SELECT VALUE id FROM uq WHERE d = 1 ORDER BY id;
-- @check
