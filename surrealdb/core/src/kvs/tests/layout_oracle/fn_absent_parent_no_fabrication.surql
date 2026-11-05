-- A child's FALLBACK never creates its parent. Rows whose parent is absent or NONE keep it that way; an
-- empty object gets the child's fallback once; an array of objects gets it once per element, and an
-- empty array gets nothing. A child's fallback under a union of object shapes must not write a key
-- that the branch the row holds does not admit.
DEFINE TABLE ab SCHEMAFULL;
DEFINE FIELD o ON ab TYPE option<object>;
DEFINE FIELD o.n ON ab TYPE option<string>;
DEFINE FIELD arr ON ab TYPE option<array<object>>;
DEFINE FIELD arr.*.n ON ab TYPE option<string>;
CREATE ab:1 RETURN NONE;
CREATE ab:3 SET o = {}, arr = [] RETURN NONE;
CREATE ab:4 SET arr = [{}, { n: 'x' }] RETURN NONE;
DEFINE TABLE ub SCHEMAFULL;
DEFINE FIELD u ON ub TYPE option<object>;
DEFINE FIELD u.n ON ub TYPE option<string>;
DEFINE FIELD u.m ON ub TYPE option<string>;
CREATE ub:1 SET u = { n: '1' } RETURN NONE;
CREATE ub:2 SET u = { m: 'x' } RETURN NONE;
CREATE ub:3 RETURN NONE;
-- @upgrade
CREATE ab:5 SET o = { n: 'q' } RETURN NONE;
ALTER FIELD o.n ON ab TYPE option<int> MIGRATE EXISTING ROWS AS n_int USING <int>$value FALLBACK -1;
ALTER FIELD arr.*.n ON ab TYPE option<int> MIGRATE EXISTING ROWS AS an_int USING <int>$value FALLBACK -1;
(INFO FOR TABLE ab).migrations;
SELECT * FROM ab ORDER BY id;
-- @check
-- @drain
SELECT * FROM ab ORDER BY id;
DEFINE INDEX by_on ON ab FIELDS o.n;
SELECT VALUE id FROM ab WHERE o.n = -1 ORDER BY id;
SELECT VALUE id FROM ab WHERE o.n = -1 EXPLAIN;
-- Refused: a scalar in the union would hold `o.n`, which only an object holds.
DEFINE FIELD OVERWRITE o ON ab TYPE option<object | string> MIGRATE EXISTING ROWS AS o_union USING $value FALLBACK NONE;
CREATE ab:7 SET o = { n: 'z' } RETURN AFTER;
-- The union of two object shapes. Each child fallback runs under the object the row holds.
ALTER FIELD u.n ON ub TYPE option<int> MIGRATE EXISTING ROWS AS un_int USING <int>$value FALLBACK -1;
(INFO FOR TABLE ub).migrations;
SELECT * FROM ub ORDER BY id;
DEFINE FIELD OVERWRITE u ON ub TYPE option<{ n: option<int> } | { m: option<string> }> MIGRATE EXISTING ROWS AS u_union USING $value FALLBACK NONE;
(INFO FOR TABLE ub).migrations;
SELECT * FROM ub ORDER BY id;
-- @drain
SELECT * FROM ub ORDER BY id;
SELECT * FROM ab ORDER BY id;
-- @check
