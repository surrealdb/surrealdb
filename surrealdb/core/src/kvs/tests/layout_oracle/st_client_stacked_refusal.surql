-- Transactions the caller owns, holding stacked migrations over two tables. A
-- refused statement does not undo what the statements before it wrote, the
-- statements after it still run, and the caller commits the lot. A refused
-- statement that changes a field's TYPE before its migration is refused must not
-- leave the new TYPE without the migration the rows need.
DEFINE TABLE t1 SCHEMAFULL;
DEFINE FIELD x ON t1 TYPE string;
DEFINE TABLE t2 SCHEMAFULL;
DEFINE FIELD y ON t2 TYPE string;
CREATE t1:1 SET x = '1' RETURN NONE;
CREATE t1:2 SET x = 'q' RETURN NONE;
CREATE t2:1 SET y = '5' RETURN NONE;
-- @upgrade
ALTER FIELD x ON t1 TYPE string MIGRATE EXISTING ROWS AS x_up USING string::uppercase($value) FALLBACK '';
-- @client
ALTER FIELD x ON t1 TYPE int MIGRATE EXISTING ROWS AS x_int USING <int>$value FALLBACK 0;
ALTER FIELD y ON t2 TYPE int MIGRATE EXISTING ROWS AS y_int USING <int>$value FALLBACK 'bad';
ALTER FIELD x ON t1 TYPE int MIGRATE EXISTING ROWS AS x_int USING <int>$value FALLBACK 0;
ALTER FIELD x ON t1 TYPE int MIGRATE EXISTING ROWS AS x_int USING <int>$value FALLBACK 1;
ALTER FIELD y ON t2 TYPE int MIGRATE EXISTING ROWS AS y_int USING <int>$value FALLBACK 0;
CREATE t2:2 SET y = 2 RETURN NONE;
-- @end
SELECT * FROM t1 ORDER BY id;
SELECT * FROM t2 ORDER BY id;
(INFO FOR TABLE t1).fields;
(INFO FOR TABLE t1).migrations;
(INFO FOR TABLE t2).fields;
(INFO FOR TABLE t2).migrations;
-- @check
-- A transaction whose first statement is refused: the statement after it still
-- runs and commits.
-- @client
ALTER FIELD y ON t2 TYPE float MIGRATE EXISTING ROWS AS y_float USING <float>$value FALLBACK 'x';
ALTER FIELD y ON t2 TYPE float MIGRATE EXISTING ROWS AS y_float USING <float>$value FALLBACK 0.5;
-- @end
SELECT * FROM t2 ORDER BY id;
(INFO FOR TABLE t2).migrations;
-- A refused TYPE change that would widen x, and a new definition of x beside it.
-- @client
ALTER FIELD x ON t1 TYPE float MIGRATE EXISTING ROWS AS x_bad USING <float>$value FALLBACK 'no';
ALTER FIELD x ON t1 TYPE float MIGRATE EXISTING ROWS AS x_widen USING <float>$value FALLBACK 0.5;
-- @end
SELECT * FROM t1 ORDER BY id;
(INFO FOR TABLE t1).fields;
(INFO FOR TABLE t1).migrations;
-- @drain
SELECT * FROM t1 ORDER BY id;
SELECT * FROM t2 ORDER BY id;
(INFO FOR TABLE t1).migrations;
(INFO FOR TABLE t2).migrations;
-- @check
-- A transaction that removes a field, redefines it and migrates it, with a
-- refusal inside it, after the job ran.
-- @client
REMOVE FIELD x ON t1;
DEFINE FIELD x ON t1 TYPE option<int>;
ALTER FIELD x ON t1 TYPE int MIGRATE EXISTING ROWS AS x_int USING <int>$value * 3 FALLBACK 0;
ALTER FIELD x ON t1 TYPE int MIGRATE EXISTING ROWS AS x_int2 USING <int>$value * 3 FALLBACK 0.5;
-- @end
SELECT * FROM t1 ORDER BY id;
(INFO FOR TABLE t1).fields;
(INFO FOR TABLE t1).migrations;
-- @check
CREATE t1:3 SET x = 3 RETURN NONE;
-- @drain
SELECT * FROM t1 ORDER BY id;
-- @check
-- A transaction whose last statement is refused, and whose statements before it
-- changed nothing that a refusal should undo: the refused definitions must leave
-- no TYPE behind, with the rows they hold still readable as the TYPE they have.
-- @client
ALTER FIELD x ON t1 TYPE option<string> MIGRATE EXISTING ROWS AS x_last USING <string>$value FALLBACK 1;
-- @end
(INFO FOR TABLE t1).fields;
(INFO FOR TABLE t1).migrations;
SELECT * FROM t1 ORDER BY id;
-- @client
DEFINE FIELD OVERWRITE y ON t2 TYPE string;
-- @end
(INFO FOR TABLE t2).fields;
SELECT * FROM t2 ORDER BY id;
-- @check
-- @drain
-- @check
