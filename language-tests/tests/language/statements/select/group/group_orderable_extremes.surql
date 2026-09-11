/**
[test]
reason = """
`array::min` / `array::max` reduce a group to its least and greatest value by
SurrealDB's own value order, so they answer over any comparable column rather
than only a numeric one. No planner-strategy override is set: both executors
owe the same answers here, and do.

They are the orderable counterpart to `math::min` / `math::max`, which fold
only numbers and report their identity (`Infinity` / `-Infinity`) for a group
that folded nothing. These report NONE instead, matching what their own scalar
forms return for an empty array, so a group with no value cannot be mistaken
for one whose extreme happens to be infinite.

`NONE` is skipped rather than ordered. It sorts below every other value, so
ordering it would make the minimum of any sparsely populated column NONE;
ignoring absent values is what SQL specifies for an aggregate. The `veg` group
below has no `w` at all and so reports NONE from both ends, while the mixed
column asserts the total order across kinds — a number sorts below a string.
"""

[[test.results]]
value = "[{ id: p:1, g: 'fruit', n: 10, w: 'pear' }]"

[[test.results]]
value = "[{ id: p:2, g: 'fruit', n: 20, w: 'apple' }]"

[[test.results]]
value = "[{ id: p:3, g: 'fruit', n: 'oops', w: 'fig' }]"

[[test.results]]
value = "[{ id: p:4, g: 'veg', n: 5 }]"

[[test.results]]
value = "[{ g: 'fruit', hi: 'pear', lo: 'apple' }, { g: 'veg', hi: NONE, lo: NONE }]"

[[test.results]]
value = "[{ g: 'fruit', hi: 'oops', lo: 10 }, { g: 'veg', hi: 5, lo: 5 }]"

[[test.results]]
value = "[{ hi: 'pear', lo: 'apple' }]"
*/

CREATE p:1 SET g = 'fruit', n = 10, w = 'pear';
CREATE p:2 SET g = 'fruit', n = 20, w = 'apple';
CREATE p:3 SET g = 'fruit', n = 'oops', w = 'fig';
CREATE p:4 SET g = 'veg', n = 5;

-- Over a string column, and over a group that carries no value at all.
SELECT array::min(w) AS lo, array::max(w) AS hi, g FROM p GROUP BY g;

-- Over a column of mixed kinds: every value is orderable against every other,
-- and a number sorts below a string.
SELECT array::min(n) AS lo, array::max(n) AS hi, g FROM p GROUP BY g;

-- GROUP ALL, where the group is every row the filter admits. Restricted to the
-- rows that carry `w`: whether an absent value is skipped or ordered is where
-- the two executors disagree, pinned by
-- `reproductions/group_aggregate_wrong_kind_{legacy,streaming}.surql`.
SELECT array::min(w) AS lo, array::max(w) AS hi FROM p WHERE g = 'fruit' GROUP ALL;
