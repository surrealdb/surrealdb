/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A materialised view maintains each aggregate from the change alone until the
value leaving a group was that group's extreme; then it has to read the rows the
group still holds and recompute the extreme from them.

Those rows are the view's, not the changed record's. A view selecting from
several tables holds one group spanning all of them, so a maximum lost from one
table is replaced from another. Reading only the table the change happened in
answers with the extreme of a fraction of the group, and where that fraction is
now empty it answers with no group's value at all — `GROUP ALL` reports the
aggregate's answer for no values, which is the bound the view must never store.

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
value = "NONE"

[[test.results]]
value = "[{ id: a:1, v: 50 }]"

[[test.results]]
value = "[{ id: b:1, v: 10 }]"

[[test.results]]
value = "[{ id: vw:[], m: 50, n: 2 }]"

[[test.results]]
value = "[{ id: a:1, v: 1 }]"

[[test.results]]
value = "[{ id: vw:[], m: 10, n: 2 }]"

[[test.results]]
value = "[{ m: 10, n: 2 }]"

[[test.results]]
value = "[{ id: a:1, v: 50 }]"

[[test.results]]
value = "[{ id: vw:[], m: 50, n: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: vw:[], m: 10, n: 1 }]"

[[test.results]]
value = "[{ m: 10, n: 1 }]"
*/

DEFINE TABLE a SCHEMALESS;
DEFINE TABLE b SCHEMALESS;
DEFINE TABLE vw AS SELECT count() AS n, math::max(v) AS m FROM a, b GROUP ALL;

-- The group spans both tables, and `a:1` holds its maximum.
CREATE a:1 SET v = 50;
CREATE b:1 SET v = 10;
SELECT * FROM vw;

-- The maximum leaves the group by being updated below it. Its replacement is in
-- the other source table.
UPDATE a:1 SET v = 1;
SELECT * FROM vw;
SELECT count() AS n, math::max(v) AS m FROM a, b GROUP ALL;

-- A new maximum needs no recalculation: it is above the stored one.
UPDATE a:1 SET v = 50;
SELECT * FROM vw;

-- The maximum leaves the group by being deleted, emptying its table while the
-- group still has a row in the other one.
DELETE a:1;
SELECT * FROM vw;
SELECT count() AS n, math::max(v) AS m FROM a, b GROUP ALL;
