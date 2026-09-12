/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A materialised view's contract is the incremental path's, not the query's: a
view row stands for a group that source rows are in, created with the group's
first row and deleted with its last. `GROUP ALL` answers a query over no rows
with the grand total, which stands for no group, so a view defined over a
source that is empty — or one a `WHERE` narrows to nothing — holds nothing
until a row arrives.

That makes `SELECT * FROM v` and the view's own query disagree over an empty
source, deliberately: the query reports the total of no rows, the table reports
the groups it holds. Closing the gap the other way would mean an emptied group
keeping a row whose extremes the incremental path cannot recompute, since the
recalculation it runs has no value to read.

The last two statements are the control on the delete side: removing the last
source row removes the group's row rather than zeroing it, which is the
invariant the initialisation has to agree with.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0, total: 0 }]"

[[test.results]]
value = "[{ id: src:1, v: 5 }]"

[[test.results]]
value = "[{ count: 1, id: total:[], total: 5 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

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
