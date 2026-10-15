-- A migration whose fallback is NONE leaves rows without a value, and a plain index over the field
-- holds no entry for them. Range, equality, IS NONE and ORDER BY ... LIMIT queries answer as the scan
-- of the rows does, lazily and eagerly, and writes that set or remove the field move rows in and out
-- of the index.
DEFINE TABLE nn SCHEMAFULL;
DEFINE FIELD a ON nn TYPE string;
CREATE nn:1 SET a = '5' RETURN NONE;
CREATE nn:2 SET a = 'junk' RETURN NONE;
CREATE nn:3 SET a = '-1' RETURN NONE;
CREATE nn:4 SET a = '' RETURN NONE;
-- @upgrade
DEFINE FIELD OVERWRITE a ON nn TYPE option<int> MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK NONE;
DEFINE INDEX ia ON nn FIELDS a;
SELECT VALUE id FROM nn WHERE a > 0 ORDER BY id;
SELECT VALUE id FROM nn WHERE a < 10 ORDER BY id;
SELECT VALUE id FROM nn WHERE a = NONE ORDER BY id;
SELECT VALUE id FROM nn WHERE a IS NONE ORDER BY id;
SELECT VALUE id FROM nn WHERE a = NULL ORDER BY id;
SELECT VALUE id FROM nn WHERE a IN [NONE, 5] ORDER BY id;
SELECT VALUE id FROM nn WHERE a = 5 ORDER BY id EXPLAIN;
SELECT VALUE id FROM nn ORDER BY a LIMIT 2;
-- @check
UPDATE nn:2 SET a = 3 RETURN NONE;
UPDATE nn:3 SET a = NONE RETURN NONE;
SELECT VALUE id FROM nn WHERE a > 0 ORDER BY id;
SELECT VALUE id FROM nn WHERE a IS NONE ORDER BY id;
-- @drain
SELECT VALUE a FROM nn ORDER BY id;
SELECT VALUE id FROM nn WHERE a < 10 ORDER BY id;
SELECT VALUE id FROM nn WHERE a IS NONE ORDER BY id;
-- @check
