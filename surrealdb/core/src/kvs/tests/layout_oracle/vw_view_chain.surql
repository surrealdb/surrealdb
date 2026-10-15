-- Property: a view whose source is another view is reached through the chain. While
-- the middle view reads the base table, the base's migrations are refused, and the middle
-- view cannot be removed while the top view is defined over it. Once the chain is removed
-- from the top, the base is free to migrate, and a chain redefined over the base is built
-- from the migrated rows, with no stale rows of the removed chain left behind.
DEFINE TABLE base SCHEMAFULL;
DEFINE FIELD n ON base TYPE string;
DEFINE FIELD k ON base TYPE string;
CREATE base:1 SET n = '3', k = 'a' RETURN NONE;
CREATE base:2 SET n = '4', k = 'b' RETURN NONE;
CREATE base:3 SET n = 'x', k = 'a' RETURN NONE;
-- @upgrade
ALTER FIELD n ON base TYPE int MIGRATE EXISTING ROWS AS n_int USING <int>$value FALLBACK 0;
DEFINE TABLE mid AS SELECT n, k FROM base;
DEFINE TABLE top AS SELECT k, math::sum(n) AS s FROM mid GROUP BY k;
SELECT * FROM mid ORDER BY id;
SELECT * FROM top ORDER BY k;
ALTER FIELD k ON base TYPE string MIGRATE EXISTING ROWS AS k_up USING string::uppercase($value) FALLBACK '';
UPDATE base:1 SET n = 10 RETURN NONE;
SELECT * FROM mid ORDER BY id;
SELECT * FROM top ORDER BY k;
-- @check
REMOVE TABLE mid;
SELECT * FROM top ORDER BY k;
REMOVE TABLE top;
REMOVE TABLE mid;
ALTER FIELD k ON base TYPE string MIGRATE EXISTING ROWS AS k_up USING string::uppercase($value) FALLBACK '';
SELECT * FROM base ORDER BY id;
DEFINE TABLE mid AS SELECT n, k FROM base;
DEFINE TABLE top AS SELECT k, math::sum(n) AS s FROM mid GROUP BY k;
SELECT * FROM mid ORDER BY id;
SELECT * FROM top ORDER BY k;
-- @drain
SELECT * FROM mid ORDER BY id;
SELECT * FROM top ORDER BY k;
-- @check
DEFINE TABLE tot AS SELECT math::sum(n) AS s, count() AS c FROM mid GROUP ALL;
ALTER FIELD n ON base TYPE int MIGRATE EXISTING ROWS AS n_plus USING $value + 1 FALLBACK 0;
REMOVE TABLE top;
REMOVE TABLE tot;
ALTER FIELD n ON base TYPE int MIGRATE EXISTING ROWS AS n_plus USING $value + 1 FALLBACK 0;
REMOVE TABLE mid;
ALTER FIELD n ON base TYPE int MIGRATE EXISTING ROWS AS n_plus USING $value + 1 FALLBACK 0;
SELECT * FROM base ORDER BY id;
DEFINE TABLE mid AS SELECT n, k FROM base;
DEFINE TABLE top AS SELECT k, math::sum(n) AS s FROM mid GROUP BY k;
SELECT * FROM top ORDER BY k;
-- @drain
SELECT * FROM top ORDER BY k;
SELECT * FROM base ORDER BY id;
-- @check
