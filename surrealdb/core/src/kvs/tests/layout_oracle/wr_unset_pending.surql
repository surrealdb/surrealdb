-- UNSET, SET NONE, CONTENT without the field, and UPSERT of an absent field on rows
-- behind a pending step: the write stores the absence, the job does not bring back the
-- value the step would have given the field, and an index defined over the migrated field
-- drops the old key of a row whose field a later write removes.
DEFINE TABLE g SCHEMAFULL;
DEFINE FIELD v ON g TYPE option<int>;
DEFINE FIELD k ON g TYPE option<int>;
CREATE g:1 SET v = 1, k = 1;
CREATE g:2 SET v = 2, k = 2;
CREATE g:3 SET v = 3, k = 3;
CREATE g:4 SET v = 4, k = 4;
-- @upgrade
CREATE g:5 SET v = 5, k = 5;
ALTER FIELD v ON g TYPE option<int> MIGRATE EXISTING ROWS AS v_plus USING $value + 100 FALLBACK 0;
DEFINE INDEX by_v ON g FIELDS v;
UPDATE g:1 SET v = NONE RETURN BEFORE;
UPDATE g:2 UNSET v RETURN BEFORE;
UPDATE g:3 SET k = 30 RETURN AFTER;
UPDATE g:4 CONTENT { k: 40 } RETURN BEFORE;
UPSERT g:6 SET v = NONE;
CREATE g:7 SET v = 7, k = 7;
BEGIN;
UPDATE g:3 SET v = NONE;
CANCEL;
BEGIN;
UPDATE g:5 UNSET v;
COMMIT;
SELECT VALUE [record::id(id), v, k] FROM g ORDER BY id;
SELECT VALUE id FROM g WHERE v > 100 ORDER BY id;
SELECT VALUE id FROM g WHERE v = 7 ORDER BY id;
-- @check
SELECT VALUE id FROM g WHERE v >= 100 ORDER BY id;
-- @drain
SELECT VALUE [record::id(id), v, k] FROM g ORDER BY id;
SELECT VALUE id FROM g WHERE v > 100 ORDER BY id;
