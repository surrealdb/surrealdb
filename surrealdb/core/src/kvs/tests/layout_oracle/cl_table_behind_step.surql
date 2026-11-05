-- A table whose rows are behind a migration step, changed in transactions the caller owns. A change of
-- the table's TYPE and a change that makes it a view are refused while the rewrite job has not
-- finished, and the refusals leave the table as it was. The statements around each refusal are kept
-- as the caller wrote them. The same changes succeed once the job has drained, and the rows read as
-- their declared types afterwards.
DEFINE TABLE vsrc SCHEMAFULL;
DEFINE FIELD f ON vsrc TYPE string;
DEFINE TABLE tt TYPE NORMAL SCHEMAFULL;
DEFINE FIELD f ON tt TYPE string;
CREATE tt:1 SET f = '1' RETURN NONE;
CREATE tt:2 SET f = '2' RETURN NONE;
-- @upgrade
ALTER FIELD f ON tt TYPE int MIGRATE EXISTING ROWS AS f_int USING <int>$value FALLBACK 0;
-- @client
ALTER TABLE tt TYPE ANY;
CREATE tt:3 SET f = 3 RETURN NONE;
DEFINE FIELD g ON tt TYPE option<string>;
UPDATE tt:1 SET g = 'x' RETURN NONE;
DEFINE TABLE OVERWRITE tt TYPE ANY SCHEMAFULL;
DEFINE TABLE OVERWRITE tt AS SELECT f FROM vsrc;
CREATE tt:4 SET f = 'bad' RETURN NONE;
-- @end
SELECT * FROM tt ORDER BY id;
(INFO FOR TABLE tt).migrations;
(INFO FOR TABLE tt).fields;
-- @check
-- @drain
SELECT * FROM tt ORDER BY id;
(INFO FOR TABLE tt).migrations;
-- @check
-- Once the job has finished, the table's TYPE change and the field added beside it commit together
-- with a failing statement after them.
-- @client
ALTER TABLE tt TYPE ANY;
UPDATE tt:3 SET g = 'after' RETURN NONE;
ALTER FIELD g ON tt TYPE int MIGRATE EXISTING ROWS AS g_int USING <int>$value FALLBACK 0;
ALTER TABLE tt TYPE NORMAL;
CREATE tt:5 SET f = 5, g = 'not int' RETURN NONE;
-- @end
SELECT * FROM tt ORDER BY id;
(INFO FOR TABLE tt).migrations;
(INFO FOR TABLE tt).fields;
-- @check
-- A view over the table is refused while a migration of the table's own field is behind a step. The
-- refused definition leaves no view behind, and the rows the caller writes afterwards are plain rows.
ALTER FIELD f ON tt TYPE string MIGRATE EXISTING ROWS AS f_str USING <string>$value FALLBACK '';
-- @client
DEFINE TABLE OVERWRITE tt AS SELECT f FROM vsrc;
CREATE tt:6 SET f = '6' RETURN NONE;
-- @end
SELECT * FROM tt ORDER BY id;
(INFO FOR TABLE tt).migrations;
-- @drain
SELECT * FROM tt ORDER BY id;
-- @check
