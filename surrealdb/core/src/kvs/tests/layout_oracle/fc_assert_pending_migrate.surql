-- An ASSERT added to a field a migration has just changed, before the job
-- reaches the rows: the assertion holds over the migrated value, the rows the
-- migration filled with its FALLBACK violate it and stay readable, the
-- verification counts them, and a write that leaves the field alone is judged
-- the same way in lazy and eager runs.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD n ON p TYPE string;
DEFINE FIELD m ON p TYPE int;
CREATE p:1 SET n = '1', m = 1 RETURN NONE;
CREATE p:2 SET n = '5', m = 2 RETURN NONE;
CREATE p:3 SET n = 'x', m = 3 RETURN NONE;
-- @upgrade
ALTER FIELD n ON p TYPE int MIGRATE EXISTING ROWS AS n_int USING <int>$value FALLBACK 0;
ALTER FIELD n ON p ASSERT $value > 3;
(INFO FOR TABLE p).fields.n;
(INFO FOR TABLE p).migrations.generation;
SELECT id, n, m FROM p ORDER BY id;
-- p:1 and p:3 hold migrated values 1 and 0, which violate the assertion; p:2 holds 5.
UPDATE p:1 SET m = 10 RETURN NONE;
UPDATE p:2 SET m = 20 RETURN NONE;
UPDATE p:2 SET n = 9 RETURN NONE;
UPDATE p:3 SET n = 0 RETURN NONE;
UPDATE p:3 SET n = 4 RETURN NONE;
SELECT id, n, m FROM p ORDER BY id;
-- @check
-- @drain
(INFO FOR TABLE p).migrations.verification;
SELECT id, n, m FROM p ORDER BY id;
UPDATE p:3 SET m = 30 RETURN NONE;
UPDATE p:1 SET n = 2 RETURN NONE;
SELECT id, n, m FROM p ORDER BY id;
-- A second ASSERT on the same field, restating the first in a different form,
-- then a changed one, then dropped.
ALTER FIELD n ON p ASSERT $value >= 0 AND $value < 100;
UPDATE p:1 SET m = 11 RETURN NONE;
UPDATE p:3 SET n = 150 RETURN NONE;
SELECT id, n, m FROM p ORDER BY id;
ALTER FIELD n ON p DROP ASSERT;
UPDATE p:1 SET n = 3 RETURN NONE;
SELECT id, n, m FROM p ORDER BY id;
-- A field added after the rows exist: rows that do not set it are not held to
-- its ASSERT, rows that do are.
DEFINE FIELD m2 ON p TYPE option<int>;
ALTER FIELD m2 ON p ASSERT $value > 100;
CREATE p:4 SET n = 6, m = 4, m2 = 5 RETURN NONE;
CREATE p:5 SET n = 6, m = 5, m2 = 500 RETURN NONE;
SELECT id, n, m, m2 FROM p ORDER BY id;
-- @check
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
