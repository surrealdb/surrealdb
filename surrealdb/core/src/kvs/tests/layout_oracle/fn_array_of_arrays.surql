-- Arrays of arrays: `grid.*.*` is an element of an element. A parent's kind cannot change while its
-- explicitly declared children disagree with it, so the element kinds here admit both int and string and
-- the migrations change values inside that kind. The inner step runs once per element and the outer step
-- once per inner array, so each element gains exactly one suffix from each step. Empty outer and inner
-- arrays and an absent grid are left alone. A second table narrows the outer kind while the element
-- definitions the array kind implies still admit the wider kind: the narrowed kind must still govern
-- what is written afterwards.
DEFINE TABLE gr SCHEMAFULL;
DEFINE FIELD grid ON gr TYPE option<array<array<int | string>>>;
DEFINE FIELD grid.* ON gr TYPE array<int | string>;
DEFINE FIELD grid.*.* ON gr TYPE int | string;
CREATE gr:1 SET grid = [['1', '2'], ['3']] RETURN NONE;
CREATE gr:2 SET grid = [[]] RETURN NONE;
CREATE gr:3 SET grid = [] RETURN NONE;
CREATE gr:4 RETURN NONE;
DEFINE TABLE gn SCHEMAFULL;
DEFINE FIELD grid ON gn TYPE option<array<array<int | string>>>;
CREATE gn:1 SET grid = [[1, 2], ['a']] RETURN NONE;
CREATE gn:2 SET grid = [[3]] RETURN NONE;
-- @upgrade
CREATE gr:5 SET grid = [['x'], ['4', 5]] RETURN NONE;
ALTER FIELD grid.*.* ON gr MIGRATE EXISTING ROWS AS inner_tag USING string::concat(<string>$value, '!') FALLBACK 'x';
(INFO FOR TABLE gr).migrations;
SELECT VALUE [<string> id, grid] FROM gr ORDER BY id;
-- @check
DEFINE FIELD OVERWRITE grid ON gr TYPE option<array<array<int | string>>> MIGRATE EXISTING ROWS AS outer_tag USING array::map($value, |$a| array::map($a, |$x| string::concat(<string>$x, '?'))) FALLBACK NONE;
(INFO FOR TABLE gr).migrations;
SELECT VALUE [<string> id, grid] FROM gr ORDER BY id;
-- Refused: the inner arrays would hold bools, which the element field does not admit.
DEFINE FIELD OVERWRITE grid.* ON gr TYPE array<bool> MIGRATE EXISTING ROWS AS mid_bool USING $value FALLBACK [];
-- Accepted: a whole-value rewrite that drops the empty inner arrays.
DEFINE FIELD OVERWRITE grid ON gr TYPE option<array<array<int | string>>> MIGRATE EXISTING ROWS AS outer_nonempty USING array::filter($value, |$a| array::len($a) > 0) FALLBACK NONE;
SELECT VALUE [<string> id, grid] FROM gr ORDER BY id;
CREATE gr:6 SET grid = [[1], [2, 'z']] RETURN AFTER;
UPDATE gr:4 SET grid = [[9]] RETURN AFTER;
-- @drain
SELECT VALUE [<string> id, grid] FROM gr ORDER BY id;
SELECT id, array::flatten(grid ?? []) AS flat FROM gr ORDER BY id;
-- @check
-- Narrow the outer kind to int. The element definitions still admit strings, and the migration's USING
-- keeps only what the new outer kind accepts.
DEFINE FIELD OVERWRITE grid ON gn TYPE option<array<array<int>>> MIGRATE EXISTING ROWS AS narrow_int USING $value FALLBACK NONE;
(INFO FOR TABLE gn).fields;
(INFO FOR TABLE gn).migrations;
SELECT VALUE [<string> id, grid] FROM gn ORDER BY id;
-- Writes after the narrowing must still be checked against the narrowed outer kind.
UPDATE gn:2 SET grid[0][0] = 'x' RETURN AFTER;
CREATE gn:3 SET grid = [['s']] RETURN AFTER;
CREATE gn:4 SET grid = [[1]] RETURN AFTER;
(INFO FOR TABLE gn).fields;
-- @drain
SELECT VALUE [<string> id, grid] FROM gn ORDER BY id;
-- @check
