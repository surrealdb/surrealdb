-- Arrays of objects: `arr.*.b` and `arr.*.c` are children of the elements of `arr`. Each element is
-- stepped once and a sibling of a stepped child survives; an empty array, NULL and an absent array
-- create no elements; an element kind that strands a child is refused.
DEFINE TABLE ao SCHEMAFULL;
DEFINE FIELD arr ON ao TYPE option<array<object>>;
DEFINE FIELD arr.*.b ON ao TYPE option<string>;
DEFINE FIELD arr.*.c ON ao TYPE option<int>;
CREATE ao:1 SET arr = [{ b: '1', c: 1 }, { b: 'x', c: 2 }, { c: 3 }] RETURN NONE;
CREATE ao:2 SET arr = [] RETURN NONE;
CREATE ao:3 SET arr = NULL RETURN NONE;
CREATE ao:4 RETURN NONE;
CREATE ao:5 SET arr = [{ b: '5' }] RETURN NONE;
-- @upgrade
CREATE ao:6 SET arr = [{ b: '6', c: 6 }, {}] RETURN NONE;
ALTER FIELD arr.*.b ON ao TYPE option<int> MIGRATE EXISTING ROWS AS b_int USING <int>$value + 1000 FALLBACK -1;
(INFO FOR TABLE ao).migrations;
SELECT * FROM ao ORDER BY id;
-- @check
-- Refused: the elements would be ints, which cannot hold `b` or `c`.
DEFINE FIELD OVERWRITE arr.* ON ao TYPE option<int> MIGRATE EXISTING ROWS AS elem_int USING $value FALLBACK NONE;
-- Refused: the element shape has no room for `c`.
DEFINE FIELD OVERWRITE arr.* ON ao TYPE option<{ b: option<int> }> MIGRATE EXISTING ROWS AS elem_drop_c USING $value FALLBACK NONE;
-- Accepted: the element shape declares both children.
DEFINE FIELD OVERWRITE arr.* ON ao TYPE option<{ b: option<int>, c: option<int> }> MIGRATE EXISTING ROWS AS elem_shape USING $value FALLBACK NONE;
(INFO FOR TABLE ao).migrations;
SELECT * FROM ao ORDER BY id;
-- Writes.
UPDATE ao:1 SET arr = [{ b: 'bad' }] RETURN AFTER;
CREATE ao:7 SET arr = [{ b: 7, c: 7, z: 1 }] RETURN AFTER;
CREATE ao:8 SET arr = [{ b: 8 }, 'str'] RETURN AFTER;
CREATE ao:9 SET arr = [{ b: 9, c: 9 }, { b: 10 }] RETURN AFTER;
UPDATE ao:5 SET arr += { b: 55 } RETURN AFTER;
DEFINE INDEX by_b ON ao FIELDS arr.*.b;
SELECT VALUE id FROM ao WHERE arr.*.b CONTAINS 1001 ORDER BY id;
SELECT VALUE id FROM ao WHERE arr.*.b CONTAINS 1001 EXPLAIN;
-- @drain
SELECT * FROM ao ORDER BY id;
SELECT VALUE [<string> id, arr.*.b, arr.*.c] FROM ao ORDER BY id;
-- @check
