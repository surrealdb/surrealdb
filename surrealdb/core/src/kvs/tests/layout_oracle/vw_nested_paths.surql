-- Property: a migration of a field inside an object (meta.score) is refused while any
-- view selects from the table, however the view reads it: by its path, through a
-- WHERE, as a group key, or by selecting the whole object. Views defined over rows
-- behind the step see the migrated nested value, and a write under the object while
-- the step is pending moves the migrated value, not the stored text.
DEFINE TABLE acct SCHEMAFULL;
DEFINE FIELD meta ON acct TYPE object;
DEFINE FIELD meta.tier ON acct TYPE string;
DEFINE FIELD meta.score ON acct TYPE string;
CREATE acct:1 SET meta = { tier: 'gold', score: '10' } RETURN NONE;
CREATE acct:2 SET meta = { tier: 'silver', score: '7' } RETURN NONE;
CREATE acct:3 SET meta = { tier: 'gold', score: 'n/a' } RETURN NONE;
-- @upgrade
ALTER FIELD meta.score ON acct TYPE int MIGRATE EXISTING ROWS AS score_int USING <int>$value FALLBACK -1;
CREATE acct:4 SET meta = { tier: 'gold', score: 4 } RETURN NONE;
DEFINE TABLE by_tier AS SELECT meta.tier AS tier, math::sum(meta.score) AS total, count() AS n FROM acct GROUP BY meta.tier;
DEFINE TABLE hi AS SELECT id, meta.score AS s FROM acct WHERE meta.score > 5;
SELECT * FROM by_tier ORDER BY tier;
SELECT * FROM hi ORDER BY id;
UPDATE acct:3 SET meta.score = 8 RETURN NONE;
UPDATE acct:1 SET meta.tier = 'silver' RETURN NONE;
SELECT * FROM by_tier ORDER BY tier;
SELECT * FROM hi ORDER BY id;
-- @check
ALTER FIELD meta.score ON acct TYPE int MIGRATE EXISTING ROWS AS score_plus USING $value + 1 FALLBACK 0;
ALTER FIELD meta.tier ON acct TYPE string MIGRATE EXISTING ROWS AS tier_up USING string::uppercase($value) FALLBACK '';
-- @drain
SELECT * FROM by_tier ORDER BY tier;
SELECT * FROM hi ORDER BY id;
REMOVE TABLE hi;
REMOVE TABLE by_tier;
DEFINE TABLE whole AS SELECT meta FROM acct;
ALTER FIELD meta.score ON acct TYPE int MIGRATE EXISTING ROWS AS score_plus USING $value + 1 FALLBACK 0;
SELECT * FROM whole ORDER BY id;
REMOVE TABLE whole;
ALTER FIELD meta.score ON acct TYPE int MIGRATE EXISTING ROWS AS score_plus USING $value + 1 FALLBACK 0;
DEFINE TABLE hi2 AS SELECT id, meta.score AS s FROM acct WHERE meta.score > 5;
SELECT * FROM hi2 ORDER BY id;
SELECT * FROM acct ORDER BY id;
-- @check
UPDATE acct:2 SET meta.score = meta.score + 10 RETURN NONE;
SELECT * FROM hi2 ORDER BY id;
-- @drain
SELECT * FROM hi2 ORDER BY id;
SELECT * FROM acct ORDER BY id;
-- @check
