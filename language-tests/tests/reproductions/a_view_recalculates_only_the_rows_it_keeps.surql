/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A materialised view holds only the rows its `WHERE` keeps, so when the value
leaving a group was that group's extreme, the recalculation replacing it has to
narrow by the view's condition as well as by the group's key. A recalculation
reading the source table unfiltered answers with the extreme of rows the view
excludes — here `src:3`, whose value is above every value the view ever held.

`GROUP BY g` is the other half of the narrowing: the rows of a group the change
did not touch must not reach the recalculation either.

Both the update and the delete reach the recalculation: an update whose new value
falls below the stored maximum, and a delete of the row holding it. Each is
followed by the view's own query, which is the answer the stored row has to
agree with.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ g: 'x', id: src:1, v: 10 }]"

[[test.results]]
value = "[{ g: 'x', id: src:2, v: 5 }]"

[[test.results]]
value = "[{ g: 'x', id: src:3, v: 500 }]"

[[test.results]]
value = "[{ g: 'y', id: src:4, v: 7 }]"

[[test.results]]
value = "[{ g: 'x', id: src:5, v: 3 }]"

[[test.results]]
value = "[{ g: 'x', id: kept:['x'], m: 10, n: 3 }, { g: 'y', id: kept:['y'], m: 7, n: 1 }]"

[[test.results]]
value = "[{ g: 'x', id: src:1, v: 1 }]"

[[test.results]]
value = "[{ g: 'x', id: kept:['x'], m: 5, n: 3 }, { g: 'y', id: kept:['y'], m: 7, n: 1 }]"

[[test.results]]
value = "[{ g: 'x', m: 5, n: 3 }, { g: 'y', m: 7, n: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ g: 'x', id: kept:['x'], m: 3, n: 2 }, { g: 'y', id: kept:['y'], m: 7, n: 1 }]"

[[test.results]]
value = "[{ g: 'x', m: 3, n: 2 }, { g: 'y', m: 7, n: 1 }]"
*/

DEFINE TABLE src SCHEMALESS;
DEFINE TABLE kept AS SELECT g, math::max(v) AS m, count() AS n FROM src WHERE v < 100 GROUP BY g;

-- `src:3` is outside the view's condition and `src:4` is in the other group, so
-- neither may reach a recalculation of group `x`.
CREATE src:1 SET g = 'x', v = 10;
CREATE src:2 SET g = 'x', v = 5;
CREATE src:3 SET g = 'x', v = 500;
CREATE src:4 SET g = 'y', v = 7;
CREATE src:5 SET g = 'x', v = 3;
SELECT * FROM kept ORDER BY g;

-- The group's maximum leaves it by being updated below it.
UPDATE src:1 SET v = 1;
SELECT * FROM kept ORDER BY g;
SELECT g, math::max(v) AS m, count() AS n FROM src WHERE v < 100 GROUP BY g ORDER BY g;

-- And the new maximum leaves it by being deleted.
DELETE src:2;
SELECT * FROM kept ORDER BY g;
SELECT g, math::max(v) AS m, count() AS n FROM src WHERE v < 100 GROUP BY g ORDER BY g;
