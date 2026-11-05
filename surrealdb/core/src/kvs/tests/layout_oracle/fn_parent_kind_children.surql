-- Changing a parent's kind while its children are defined. Rows whose parent is an object, an empty
-- object, NULL or absent keep what they hold; a child's non-idempotent step runs once per row; a
-- parent kind that strands a child, or a child kind its parent's literal shape rejects, is refused.
DEFINE TABLE pk SCHEMAFULL;
DEFINE FIELD o ON pk TYPE option<object>;
DEFINE FIELD o.n ON pk TYPE option<string>;
DEFINE FIELD o.s ON pk TYPE option<string>;
CREATE pk:1 SET o = { n: '1', s: 'a' } RETURN NONE;
CREATE pk:2 SET o = { s: 'b' } RETURN NONE;
CREATE pk:3 SET o = {} RETURN NONE;
CREATE pk:4 SET o = NULL RETURN NONE;
CREATE pk:5 RETURN NONE;
-- @upgrade
CREATE pk:6 SET o = { n: '6', s: 'c' } RETURN NONE;
ALTER FIELD o.n ON pk TYPE option<int> MIGRATE EXISTING ROWS AS n_int USING <int>$value + 1000 FALLBACK -1;
(INFO FOR TABLE pk).migrations;
SELECT * FROM pk ORDER BY id;
-- @check
-- Accepted: a literal shape that declares both children, which the stepped values already fit.
DEFINE FIELD OVERWRITE o ON pk TYPE option<{ n: option<int>, s: option<string> }> MIGRATE EXISTING ROWS AS o_shape USING $value FALLBACK NONE;
(INFO FOR TABLE pk).migrations;
-- Refused: the literal shape has no room for `s`.
DEFINE FIELD OVERWRITE o ON pk TYPE option<{ n: option<int> }> MIGRATE EXISTING ROWS AS o_drop_s USING $value FALLBACK NONE;
-- Refused: an array of objects cannot hold `o.n` or `o.s`.
DEFINE FIELD OVERWRITE o ON pk TYPE option<array<object>> MIGRATE EXISTING ROWS AS o_arr USING [] FALLBACK NONE;
-- Refused: `o.s` may not change to int under a shape that declares it string.
ALTER FIELD o.s ON pk TYPE option<int> MIGRATE EXISTING ROWS AS s_int USING <int>$value FALLBACK 0;
-- Accepted: a wider shape with a field no row holds yet.
DEFINE FIELD OVERWRITE o ON pk TYPE option<{ n: option<int>, s: option<string>, t: option<bool> }> MIGRATE EXISTING ROWS AS o_wide USING $value FALLBACK NONE;
(INFO FOR TABLE pk).migrations;
-- Writes against the new shape.
CREATE pk:7 SET o = { n: 'x' } RETURN AFTER;
UPDATE pk:1 SET o.n = '9' RETURN AFTER;
UPDATE pk:2 SET o.n = 9 RETURN AFTER;
DEFINE INDEX by_n ON pk FIELDS o.n;
SELECT VALUE id FROM pk WHERE o.n = 1001 ORDER BY id;
SELECT VALUE id FROM pk WHERE o.n = 1001 EXPLAIN;
-- @drain
SELECT * FROM pk ORDER BY id;
SELECT VALUE [<string> id, o.n, o.s] FROM pk ORDER BY id;
-- @check
