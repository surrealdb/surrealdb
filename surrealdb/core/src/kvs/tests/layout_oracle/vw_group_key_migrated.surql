-- Property: a GROUP BY view defined over rows behind a migration step groups them by
-- the migrated value of the key and sums the migrated values. A write to a pending row
-- moves it between groups by its migrated key, and the groups agree after the job runs.
-- Once the views are removed, a second migration of a summed field (qty) is accepted,
-- and a view defined after it sums the re-migrated values.
DEFINE TABLE sale SCHEMAFULL;
DEFINE FIELD code ON sale TYPE string;
DEFINE FIELD qty ON sale TYPE int;
CREATE sale:1 SET code = '1', qty = 10 RETURN NONE;
CREATE sale:2 SET code = '2', qty = 20 RETURN NONE;
CREATE sale:3 SET code = 'junk', qty = 30 RETURN NONE;
CREATE sale:4 SET code = '1', qty = 40 RETURN NONE;
-- @upgrade
ALTER FIELD code ON sale TYPE int MIGRATE EXISTING ROWS AS code_int USING <int>$value FALLBACK 0;
CREATE sale:5 SET code = 2, qty = 50 RETURN NONE;
DEFINE TABLE by_code AS SELECT code, math::sum(qty) AS total, count() AS n FROM sale GROUP BY code;
DEFINE TABLE by_all AS SELECT math::sum(qty) AS total, math::max(qty) AS top, count() AS n FROM sale GROUP ALL;
SELECT * FROM by_code ORDER BY code;
SELECT * FROM by_all;
UPDATE sale:3 SET qty = 33 RETURN NONE;
UPDATE sale:1 SET code = 2 RETURN NONE;
SELECT * FROM by_code ORDER BY code;
-- @check
DELETE sale:4;
SELECT * FROM by_code ORDER BY code;
SELECT * FROM by_all;
-- @drain
SELECT * FROM by_code ORDER BY code;
SELECT * FROM by_all;
UPDATE sale:3 SET code = 1 RETURN NONE;
SELECT * FROM by_code ORDER BY code;
-- @check
REMOVE TABLE by_code;
REMOVE TABLE by_all;
ALTER FIELD qty ON sale TYPE int MIGRATE EXISTING ROWS AS qty_x2 USING $value * 2 FALLBACK 0;
DEFINE TABLE by_code AS SELECT code, math::sum(qty) AS total, count() AS n FROM sale GROUP BY code;
SELECT * FROM by_code ORDER BY code;
SELECT * FROM sale ORDER BY id;
-- @drain
SELECT * FROM by_code ORDER BY code;
-- @check
