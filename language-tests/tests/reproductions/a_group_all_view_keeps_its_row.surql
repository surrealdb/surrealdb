/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A materialised view's `GROUP ALL` row is the one group of the whole source, and it
is held from the view's definition on: a source that is empty, or that a `WHERE`
narrows to nothing, still gives the row, with the aggregates of no rows (`count`
is `0`), as the view's own query does. Removing the last source row empties that
row rather than removing it. A view over `GROUP BY` keys is different: each of its
rows stands for a group that source rows are in, is created with the group's first
row and is deleted with its last.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 0, id: total:[], total: 0 }]"
[[test.results]]
value = "[{ count: 0, id: big:[] }]"
[[test.results]]
value = "[{ count: 0, total: 0 }]"

[[test.results]]
value = "[{ id: src:1, v: 5 }]"

[[test.results]]
value = "[{ count: 1, id: total:[], total: 5 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0, id: total:[], total: 0 }]"
[[test.results]]
value = "[{ count: 0, id: big:[] }]"
[[test.results]]
value = "[{ count: 0, total: 0 }]"
*/

DEFINE TABLE src SCHEMALESS;
DEFINE TABLE total AS SELECT count() AS count, math::sum(v) AS total FROM src GROUP ALL;
DEFINE TABLE big AS SELECT count() AS count FROM src WHERE v > 100 GROUP ALL;

-- The view is defined over a source with no rows, so it holds no group.
SELECT * FROM total;
SELECT * FROM big;

-- The query the view is defined as reports the total of no rows.
SELECT count() AS count, math::sum(v) AS total FROM src GROUP ALL;

-- The group's first row creates the view's row.
CREATE src:1 SET v = 5;
SELECT * FROM total;

-- The group's last row removes it again, and the narrower view never had one.
DELETE src:1;
SELECT * FROM total;
SELECT * FROM big;
SELECT count() AS count, math::sum(v) AS total FROM src GROUP ALL;
