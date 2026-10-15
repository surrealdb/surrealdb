-- A three-level path, a.b.c. A child's step and a later step on the object that holds it each apply
-- once per row: a child's value gains exactly one increment from each step. Sibling keys at every level
-- survive. An absent middle object is not created by a child's fallback, and a step on the outer
-- FLEXIBLE object leaves the nested object and its undeclared sibling untouched.
DEFINE TABLE dc SCHEMAFULL;
DEFINE FIELD a ON dc TYPE option<object> FLEXIBLE;
DEFINE FIELD a.b ON dc TYPE option<object> FLEXIBLE;
DEFINE FIELD a.b.c ON dc TYPE option<string>;
DEFINE FIELD a.b.e ON dc TYPE option<string>;
CREATE dc:1 SET a = { b: { c: '1', d: 'keep', e: 'ab' }, x: 'keep' } RETURN NONE;
CREATE dc:2 SET a = { x: 'only' } RETURN NONE;
CREATE dc:3 SET a = { b: {} } RETURN NONE;
CREATE dc:4 SET a = {} RETURN NONE;
CREATE dc:5 SET a = { b: { c: '5' } } RETURN NONE;
-- @upgrade
ALTER FIELD a.b.c ON dc TYPE option<int> MIGRATE EXISTING ROWS AS c_int USING <int>$value * 10 + 1 FALLBACK -1;
(INFO FOR TABLE dc).migrations;
SELECT * FROM dc ORDER BY id;
-- @check
-- Accepted: the middle object becomes a literal shape declaring the undeclared `d`, rewriting nothing.
DEFINE FIELD OVERWRITE a.b ON dc TYPE option<{ c: option<int>, d: option<string>, e: option<string> }> MIGRATE EXISTING ROWS AS b_shape USING $value FALLBACK NONE;
(INFO FOR TABLE dc).migrations;
SELECT * FROM dc ORDER BY id;
-- The parent's step adds 1000 to c once, after the child's step has multiplied it.
DEFINE FIELD OVERWRITE a.b ON dc TYPE option<{ c: option<int>, d: option<string>, e: option<string> }> MIGRATE EXISTING ROWS AS b_bump USING object::extend($value, { c: $value.c + 1000 }) FALLBACK NONE;
(INFO FOR TABLE dc).migrations;
SELECT * FROM dc ORDER BY id;
-- Refused: the shape has no room for `e`.
DEFINE FIELD OVERWRITE a.b ON dc TYPE option<{ c: option<int>, d: option<string> }> MIGRATE EXISTING ROWS AS b_drop_e USING $value FALLBACK NONE;
-- Refused: `e` may not change to int under the shape that declares it as a string.
ALTER FIELD a.b.e ON dc TYPE option<int> MIGRATE EXISTING ROWS AS e_int USING <int>$value FALLBACK 0;
-- Accepted: a step on the outer FLEXIBLE object leaves the nested shape and the sibling `x` alone.
DEFINE FIELD OVERWRITE a ON dc TYPE option<object> FLEXIBLE MIGRATE EXISTING ROWS AS a_x USING object::extend($value, { x: 'X' }) FALLBACK NONE;
(INFO FOR TABLE dc).migrations;
SELECT * FROM dc ORDER BY id;
DEFINE INDEX by_c ON dc FIELDS a.b.c;
SELECT VALUE id FROM dc WHERE a.b.c = 1011 ORDER BY id;
SELECT VALUE id FROM dc WHERE a.b.c = 1011 EXPLAIN;
UPDATE dc:1 SET a.b.c = 5 RETURN AFTER;
-- @drain
SELECT * FROM dc ORDER BY id;
SELECT VALUE [<string> id, a.b.c, a.b.e, a.b.d] FROM dc ORDER BY id;
-- @check
