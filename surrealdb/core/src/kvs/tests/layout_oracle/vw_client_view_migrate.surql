-- Property: a view and a migration decided in one transaction the caller owns. A
-- refused statement leaves no step, though the statements before it are kept. A view
-- defined after a migration minted in the same transaction is built from the migrated
-- values. Removing a view and migrating its source in the same transaction takes effect
-- together, and the rows read as their declared field types after the commit.
DEFINE TABLE src SCHEMAFULL;
DEFINE FIELD a ON src TYPE string;
DEFINE FIELD g ON src TYPE string;
CREATE src:1 SET a = '5', g = 'x' RETURN NONE;
CREATE src:2 SET a = 'junk', g = 'x' RETURN NONE;
CREATE src:3 SET a = '7', g = 'y' RETURN NONE;
-- @upgrade
-- @client
ALTER FIELD a ON src TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
DEFINE TABLE tot AS SELECT g, math::sum(a) AS s FROM src GROUP BY g;
SELECT * FROM tot ORDER BY g;
ALTER FIELD g ON src TYPE string MIGRATE EXISTING ROWS AS g_up USING string::uppercase($value) FALLBACK '';
CREATE src:4 SET a = 4, g = 'y' RETURN NONE;
-- @end
SELECT * FROM tot ORDER BY g;
SELECT * FROM src ORDER BY id;
(INFO FOR TABLE src).migrations;
-- @check
-- @client
REMOVE TABLE tot;
ALTER FIELD g ON src TYPE string MIGRATE EXISTING ROWS AS g_up USING string::uppercase($value) FALLBACK '';
DEFINE TABLE tot2 AS SELECT g, math::sum(a) AS s FROM src GROUP BY g;
UPDATE src:2 SET a = 2 RETURN NONE;
-- @end
SELECT * FROM tot2 ORDER BY g;
SELECT * FROM src ORDER BY id;
-- @drain
SELECT * FROM tot2 ORDER BY g;
-- @client
ALTER FIELD a ON src TYPE int MIGRATE EXISTING ROWS AS a_x10 USING $value * 10 FALLBACK 0;
REMOVE TABLE tot2;
ALTER FIELD a ON src TYPE int MIGRATE EXISTING ROWS AS a_x10 USING $value * 10 FALLBACK 0;
DEFINE TABLE tot3 AS SELECT g, math::sum(a) AS s FROM src GROUP BY g;
-- @end
SELECT * FROM tot3 ORDER BY g;
SELECT * FROM src ORDER BY id;
(INFO FOR TABLE src).migrations;
-- @drain
SELECT * FROM tot3 ORDER BY g;
SELECT * FROM src ORDER BY id;
-- @check
