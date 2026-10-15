-- Transforms over absent values and transforms whose result is NONE. A row that
-- lacks the field is handed NONE as $value; a result of NONE removes the field,
-- which only a TYPE that admits NONE may hold; a result of NONE for a required
-- type is not a value of that type and takes the FALLBACK.
DEFINE TABLE ab SCHEMAFULL;
DEFINE FIELD c ON ab TYPE option<int>;
DEFINE FIELD d ON ab TYPE option<string>;
CREATE ab:1 SET c = 1, d = 'x' RETURN NONE;
CREATE ab:2 SET d = 'y' RETURN NONE;
CREATE ab:3 SET c = 8 RETURN NONE;
CREATE ab:4 RETURN NONE;
-- @upgrade
ALTER FIELD c ON ab TYPE option<int> MIGRATE EXISTING ROWS AS c_default USING $value ?? 0 FALLBACK 0;
SELECT * FROM ab ORDER BY id;
ALTER FIELD c ON ab TYPE option<int> MIGRATE EXISTING ROWS AS c_drop USING IF $value > 5 THEN NONE ELSE $value END FALLBACK NONE;
SELECT * FROM ab ORDER BY id;
SELECT VALUE id FROM ab WHERE c IS NONE ORDER BY id;
-- @check
ALTER FIELD d ON ab TYPE string MIGRATE EXISTING ROWS AS d_req USING $value ?? 'empty' FALLBACK 'f';
SELECT * FROM ab ORDER BY id;
-- A required type whose transform yields NONE for one row: that row takes the
-- FALLBACK.
ALTER FIELD d ON ab TYPE string MIGRATE EXISTING ROWS AS d_none USING IF $value = 'x' THEN NONE ELSE <string>$value END FALLBACK 'g';
SELECT * FROM ab ORDER BY id;
-- @drain
SELECT * FROM ab ORDER BY id;
(INFO FOR TABLE ab).migrations;
-- @check
-- A field the rows mostly lack, given a required type with a transform that
-- passes NONE through: the absent rows take the FALLBACK.
DEFINE FIELD e ON ab TYPE option<int>;
CREATE ab:5 SET c = 2, d = 'z' RETURN NONE;
ALTER FIELD e ON ab TYPE int MIGRATE EXISTING ROWS AS e_none USING $value FALLBACK 1;
SELECT * FROM ab ORDER BY id;
-- A transform that errors on NONE, because the cast refuses it, takes the
-- FALLBACK for every absent row.
DEFINE FIELD f ON ab TYPE option<int>;
ALTER FIELD f ON ab TYPE int MIGRATE EXISTING ROWS AS f_cast USING <int>$value FALLBACK 2;
SELECT * FROM ab ORDER BY id;
-- @drain
SELECT * FROM ab ORDER BY id;
SELECT VALUE id FROM ab WHERE e = 1 AND f = 2 ORDER BY id;
(INFO FOR TABLE ab).migrations;
-- @check
-- Two steps queued in one batch on a field: the first turns large values into NONE
-- (removing the field), and the second is handed that NONE as $value and fills it.
-- Rows the first step did not touch keep their value through both.
DEFINE FIELD g ON ab TYPE option<int>;
CREATE ab:8 SET g = 5, d = 'a', e = 8, f = 8 RETURN NONE;
CREATE ab:9 SET g = 50, d = 'b', e = 9, f = 9 RETURN NONE;
CREATE ab:10 SET d = 'c', e = 10, f = 10 RETURN NONE;
ALTER FIELD g ON ab TYPE option<int> MIGRATE EXISTING ROWS AS g_drop USING IF $value > 10 THEN NONE ELSE $value END FALLBACK NONE;
ALTER FIELD g ON ab TYPE int MIGRATE EXISTING ROWS AS g_fill USING $value ?? 99 FALLBACK 0;
SELECT VALUE g FROM ab WHERE id IN [ab:8, ab:9, ab:10] ORDER BY id;
-- @drain
SELECT VALUE g FROM ab WHERE id IN [ab:8, ab:9, ab:10] ORDER BY id;
(INFO FOR TABLE ab).migrations;
-- @check
-- A row written now must satisfy the types it was written against: a NONE for
-- the required field is refused, so the migration has nothing to repair.
CREATE ab:6 SET c = 6, d = 'w', e = 6, f = 6 RETURN NONE;
CREATE ab:7 SET c = 7, d = 'v', e = 7 RETURN NONE;
SELECT * FROM ab ORDER BY id;
-- @drain
-- @check
