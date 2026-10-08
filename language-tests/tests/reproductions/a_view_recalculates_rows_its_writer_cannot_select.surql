/**
[env]
namespace = true
database = true
imports = ["reproductions/a_view_recalculates_rows_its_writer_cannot_select_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "user:writer" }

[test]
reason = """
An aggregated view's recalculation reads the whole group, whatever the writer
whose statement triggered it may select. The value it computes is stored on the
view and read by everyone afterwards, so computing it under one writer's
`PERMISSIONS FOR select` would persist that writer's partial answer as the
view's own.

Both halves of the damage are here. A group that still holds a row the writer
may select stores the maximum of the rows it may select, silently replacing the
group's real maximum. A group that holds none leaves the recalculation with no
row at all, and the delete that triggered it fails.
"""

[[test.results]]
value = "[{ v: 3 }]"

[[test.results]]
value = "[{ m: 10, n: 3, sensor: 's' }, { m: 9, n: 2, sensor: 't' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ m: 7, n: 2, sensor: 's' }, { m: 9, n: 2, sensor: 't' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ m: 7, n: 2, sensor: 's' }, { m: 4, n: 1, sensor: 't' }]"
*/

-- One row of the five is selectable here, which is what makes a recalculation
-- narrowed to this session's permissions answer differently from the view's.
SELECT v FROM reading ORDER BY v;
SELECT m, n, sensor FROM peak ORDER BY sensor;

-- The row holding group `s`'s maximum leaves the group. The recalculation has
-- to reach `reading:s2`, which this session may not select.
DELETE reading:s1;
SELECT m, n, sensor FROM peak ORDER BY sensor;

-- Group `t` keeps no row this session may select, so a narrowed recalculation
-- has nothing left to recalculate over.
DELETE reading:t1;
SELECT m, n, sensor FROM peak ORDER BY sensor;
