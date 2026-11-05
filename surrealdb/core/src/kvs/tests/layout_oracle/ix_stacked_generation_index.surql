-- Two migrations stack on one field with an index defined between them. The index over the first
-- migration's values blocks the second, so the second is applied only after the index is removed, and
-- the index is then defined over the twice-migrated values. Rows behind both steps are replayed through
-- the first and then the second, so ranges and equality must see n + 100 for rows the job has not reached.
DEFINE TABLE sg SCHEMAFULL;
DEFINE FIELD n ON sg TYPE string;
CREATE sg:1 SET n = '5' RETURN NONE;
CREATE sg:2 SET n = '40' RETURN NONE;
CREATE sg:3 SET n = 'x' RETURN NONE;
CREATE sg:4 SET n = '-2' RETURN NONE;
-- @upgrade
ALTER FIELD n ON sg TYPE int MIGRATE EXISTING ROWS AS n_int USING <int>$value FALLBACK 0;
CREATE sg:5 SET n = 9 RETURN NONE;
DEFINE INDEX gi ON sg FIELDS n;
ALTER FIELD n ON sg TYPE int MIGRATE EXISTING ROWS AS n_plus USING $value + 100 FALLBACK 0;
(INFO FOR TABLE sg).migrations;
REMOVE INDEX gi ON sg;
ALTER FIELD n ON sg TYPE int MIGRATE EXISTING ROWS AS n_plus USING $value + 100 FALLBACK 0;
DEFINE INDEX gi ON sg FIELDS n;
SELECT VALUE id FROM sg WHERE n > 100 ORDER BY id;
SELECT VALUE id FROM sg WHERE n = 105 ORDER BY id;
SELECT VALUE id FROM sg WHERE n >= 100 AND n < 110 ORDER BY id;
SELECT VALUE id FROM sg WHERE n >= 100 AND n < 110 ORDER BY id EXPLAIN;
SELECT VALUE n FROM sg ORDER BY id;
-- @check
UPDATE sg:2 SET n = 1 RETURN NONE;
CREATE sg:6 SET n = 7 RETURN NONE;
SELECT VALUE id FROM sg WHERE n < 10 ORDER BY id;
SELECT VALUE id FROM sg WHERE n > 100 ORDER BY id;
-- @drain
SELECT VALUE n FROM sg ORDER BY id;
SELECT VALUE id FROM sg WHERE n > 100 ORDER BY id;
(INFO FOR TABLE sg).migrations;
-- @check
