-- Writes inside BEGIN ... COMMIT, BEGIN ... CANCEL and a transaction the caller owns,
-- while steps are pending. A cancelled block leaves every row as the steps left it, a
-- committed block applies each step once, and a migration minted in a committed block
-- reaches the rows created earlier in that block and the rows it writes.
DEFINE TABLE tx SCHEMAFULL;
DEFINE FIELD n ON tx TYPE string;
DEFINE FIELD tag ON tx TYPE string;
CREATE tx:1 SET n = '1', tag = 'a';
CREATE tx:2 SET n = '2', tag = 'b';
CREATE tx:3 SET n = '3', tag = 'c';
-- @upgrade
CREATE tx:4 SET n = '4', tag = 'd';
ALTER FIELD n ON tx TYPE int MIGRATE EXISTING ROWS AS n_plus USING <int>$value + 10 FALLBACK -1;
BEGIN;
UPDATE tx:1 SET n += 1;
UPDATE tx:2 SET tag = 'bb';
CANCEL;
SELECT VALUE [record::id(id), n, tag] FROM tx ORDER BY id;
BEGIN;
UPDATE tx:1 SET n += 1 RETURN BEFORE;
UPDATE tx:2 SET tag = 'bb';
UPDATE tx:3 SET n -= 3 RETURN AFTER;
COMMIT;
BEGIN;
CREATE tx:5 SET n = 5, tag = 'e';
ALTER FIELD tag ON tx TYPE string MIGRATE EXISTING ROWS AS tag_up USING string::uppercase($value) FALLBACK '';
UPDATE tx:4 SET n += 1;
UPDATE tx:3 SET tag = 'cc';
COMMIT;
BEGIN;
ALTER FIELD n ON tx TYPE int MIGRATE EXISTING ROWS AS n_bad USING $value * 3 FALLBACK 0;
UPDATE tx:4 SET tag = 'zz';
CANCEL;
SELECT VALUE [record::id(id), n, tag] FROM tx ORDER BY id;
-- @check
-- @client
UPDATE tx:1 SET n += 1;
UPDATE tx:3 SET n += 1;
UPDATE tx:2 SET n = 'x';
UPDATE tx:4 SET n += 1;
-- @end
SELECT VALUE [record::id(id), n, tag] FROM tx ORDER BY id;
-- @drain
SELECT VALUE [record::id(id), n, tag] FROM tx ORDER BY id;
