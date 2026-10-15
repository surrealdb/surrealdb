-- Every refusal a migration's transform, FALLBACK or target field can raise, each in a transaction the
-- caller owns and between writes the caller keeps. A valid migration of the same field follows them,
-- and a failing write ends the transaction. The refused migrations mint nothing: their names are free
-- for a later statement, the field keeps its TYPE until the valid migration, and the rows the caller
-- committed read as their declared types.
DEFINE FUNCTION fn::conv($v: any) { RETURN <int>$v };
DEFINE TABLE fb SCHEMAFULL;
DEFINE FIELD a ON fb TYPE string;
DEFINE FIELD b ON fb TYPE string;
DEFINE FIELD c ON fb TYPE option<string>;
CREATE fb:1 SET a = '1', b = 'x', c = 'k' RETURN NONE;
CREATE fb:2 SET a = 'q', b = 'y' RETURN NONE;
-- @upgrade
-- @client
CREATE fb:3 SET a = '3', b = 'z' RETURN NONE;
ALTER FIELD a ON fb TYPE int MIGRATE EXISTING ROWS AS a1 USING <int>$value FALLBACK 'zero';
ALTER FIELD a ON fb TYPE int MIGRATE EXISTING ROWS AS a2 USING <int>$value;
ALTER FIELD a ON fb TYPE int MIGRATE EXISTING ROWS AS a3 USING <int>time::now() FALLBACK 0;
ALTER FIELD a ON fb TYPE int MIGRATE EXISTING ROWS AS a4 USING <int>$value FALLBACK $value;
ALTER FIELD a ON fb TYPE int MIGRATE EXISTING ROWS AS a5 USING fn::conv($value) FALLBACK 0;
CREATE fb:4 SET a = '4', b = 'w' RETURN NONE;
ALTER FIELD c ON fb TYPE int MIGRATE EXISTING ROWS AS c1 USING <int>$value FALLBACK 0;
ALTER FIELD b ON fb TYPE int MIGRATE EXISTING ROWS AS b1 USING <int>$value FALLBACK 0;
ALTER FIELD a ON fb TYPE int MIGRATE EXISTING ROWS AS a6 USING <int>$value FALLBACK 0;
CREATE fb:5 SET a = 5, b = 'w' RETURN NONE;
CREATE fb:6 SET a = 6, b = 6 RETURN NONE;
-- @end
SELECT * FROM fb ORDER BY id;
SELECT VALUE a FROM fb WHERE a > 1 ORDER BY a;
(INFO FOR TABLE fb).fields;
(INFO FOR TABLE fb).migrations;
-- @check
-- @drain
SELECT * FROM fb ORDER BY id;
(INFO FOR TABLE fb).migrations;
-- @check
-- A name a refused migration of an earlier transaction gave is free: a migration of the converted
-- field under it is a new step, and a refused one of the other field is not counted.
-- @client
ALTER FIELD a ON fb TYPE int MIGRATE EXISTING ROWS AS a1 USING <int>$value * 10 FALLBACK 0;
ALTER FIELD b ON fb TYPE string MIGRATE EXISTING ROWS AS b_back USING <string>$value FALLBACK '';
CREATE fb:7 SET a = 7, b = 'seven' RETURN NONE;
ALTER FIELD c ON fb TYPE int MIGRATE EXISTING ROWS AS c1 USING <int>$value FALLBACK 0;
-- @end
SELECT * FROM fb ORDER BY id;
(INFO FOR TABLE fb).migrations;
-- @drain
SELECT * FROM fb ORDER BY id;
-- @check
