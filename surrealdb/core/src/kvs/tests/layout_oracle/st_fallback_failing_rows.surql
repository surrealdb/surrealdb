-- A transform that fails for some rows takes its FALLBACK for exactly those rows,
-- whatever kind of value the row holds: a string that is not a number, a float,
-- a bool, an array, an object, the largest integer, NULL, an absent field and a
-- NaN string. A FALLBACK that does not pass the field's definition is refused,
-- and a transform with no FALLBACK is accepted only when it cannot fail.
DEFINE TABLE fb SCHEMAFULL;
DEFINE FIELD n ON fb TYPE any;
CREATE fb:1 SET n = '12' RETURN NONE;
CREATE fb:2 SET n = 'x' RETURN NONE;
CREATE fb:3 SET n = 12.7 RETURN NONE;
CREATE fb:4 SET n = true RETURN NONE;
CREATE fb:5 SET n = [1, 2] RETURN NONE;
CREATE fb:6 SET n = { a: 1 } RETURN NONE;
CREATE fb:7 SET n = 9223372036854775807 RETURN NONE;
CREATE fb:8 SET n = NULL RETURN NONE;
CREATE fb:9 RETURN NONE;
CREATE fb:10 SET n = 'NaN' RETURN NONE;
DEFINE FIELD s ON fb TYPE option<int>;
CREATE fb:11 SET s = 1 RETURN NONE;
-- @upgrade
UPDATE fb SET s = 1 WHERE s IS NONE RETURN NONE;
ALTER FIELD n ON fb TYPE option<int> MIGRATE EXISTING ROWS AS n_int USING <int>$value FALLBACK NONE;
SELECT id, n FROM fb ORDER BY id;
SELECT VALUE n FROM fb ORDER BY id;
-- @check
ALTER FIELD n ON fb TYPE int MIGRATE EXISTING ROWS AS n_req USING <int>$value FALLBACK 0;
SELECT id, n FROM fb ORDER BY id;
-- Refused: a FALLBACK the field's type does not admit, a NONE fallback for a
-- required type, a float fallback for an int type, and a transform that can fail
-- with no FALLBACK.
ALTER FIELD n ON fb TYPE int MIGRATE EXISTING ROWS AS bad1 USING <int>$value FALLBACK 'x';
ALTER FIELD n ON fb TYPE int MIGRATE EXISTING ROWS AS bad2 USING <int>$value FALLBACK NONE;
ALTER FIELD n ON fb TYPE int MIGRATE EXISTING ROWS AS bad3 USING <int>$value FALLBACK 2.5;
ALTER FIELD n ON fb TYPE int MIGRATE EXISTING ROWS AS bad4 USING <int>$value;
SELECT id, n FROM fb ORDER BY id;
(INFO FOR TABLE fb).fields;
(INFO FOR TABLE fb).migrations;
-- Accepted: a transform that cannot fail, whose result always passes the type.
-- Every row holds an int in s, so the cast cannot fail and float admits the result.
ALTER FIELD s ON fb TYPE option<float> MIGRATE EXISTING ROWS AS s_float USING <float>$value;
SELECT id, s FROM fb ORDER BY id;
(INFO FOR TABLE fb).fields;
-- A float-typed field whose transform fails on the strings and the arrays it holds.
ALTER FIELD n ON fb TYPE float MIGRATE EXISTING ROWS AS n_float USING <float>$value FALLBACK 0.5;
SELECT id, n FROM fb ORDER BY id;
-- @drain
SELECT id, n, s FROM fb ORDER BY id;
(INFO FOR TABLE fb).migrations;
-- @check
-- A row written after the job ran is stamped with the current generation, so no
-- step touches it; it holds the values its types admit.
CREATE fb:12 SET n = 3.5f, s = 2f RETURN NONE;
SELECT id, n, s FROM fb ORDER BY id;
-- @drain
-- @check
