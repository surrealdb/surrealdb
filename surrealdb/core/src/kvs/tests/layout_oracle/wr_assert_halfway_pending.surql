-- A multi-row UPDATE that fails an ASSERT on a row it reaches after writing others
-- leaves every row as the steps left it when it is a statement of its own. A row whose
-- migrated value breaks an ASSERT added after the migration stays readable, and a write
-- that touches the record is refused while the value still breaks it. Inside a
-- client-owned transaction the later statements still run, and the writes the failed
-- statement made before the failing row refuse the transaction's commit, so it leaves
-- every row as it was.
DEFINE TABLE a SCHEMAFULL;
DEFINE FIELD n ON a TYPE string;
DEFINE FIELD tag ON a TYPE option<string>;
CREATE a:1 SET n = '10';
CREATE a:2 SET n = '20';
CREATE a:3 SET n = '30';
CREATE a:4 SET n = '40';
CREATE a:6 SET n = '70';
-- @upgrade
CREATE a:5 SET n = '50';
ALTER FIELD n ON a TYPE int MIGRATE EXISTING ROWS AS n_plus USING <int>$value + 5 FALLBACK 0;
ALTER FIELD n ON a TYPE int ASSERT $value < 60;
SELECT VALUE [record::id(id), n] FROM a ORDER BY id;
UPDATE a SET n += 10;
UPDATE a SET tag = 'x' WHERE id < a:3;
UPDATE a SET n += 3 WHERE id > a:2 AND id < a:6;
UPDATE a SET n += 3 WHERE id > a:2 AND id < a:6;
UPDATE a:6 SET tag = 'q';
UPDATE a:5 SET n = 99;
BEGIN;
UPDATE a:1 SET n += 1;
UPDATE a SET n += 10;
COMMIT;
SELECT VALUE [record::id(id), n, tag] FROM a ORDER BY id;
-- @check
-- @client
UPDATE a:1 SET n += 1;
UPDATE a SET n += 10;
UPDATE a:2 SET n += 1;
-- @end
SELECT VALUE [record::id(id), n, tag] FROM a ORDER BY id;
-- @drain
SELECT VALUE [record::id(id), n, tag] FROM a ORDER BY id;
