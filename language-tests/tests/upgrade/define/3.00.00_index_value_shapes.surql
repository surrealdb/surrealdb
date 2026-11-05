/**
[env]
imports = ["upgrade/define/3.00.00_index_value_shapes_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
reason = """
A B-tree index records the value shapes it stores — whether a column has held an
array — from the build that starts it. An index an older binary built recorded
none, so its columns may hold either shape, and a read must not take a column
whose shapes it does not know for one that never held an array: the array
record's one entry sits at its element, below every datetime, while its whole
value sorts above them. Each indexed read answers what its WITH NOINDEX twin
answers. The plans, and the plan a rebuild restores, are asserted in the
_current sibling.

The upgrade target is bounded to the current checkout: a released binary's
planner takes the key order of such a column for its order, so these reads are
not what its table scan answers there."""

[[test.results]]
value = "[shape_mixed:arr, shape_mixed:3]"

[[test.results]]
value = "[shape_mixed:arr, shape_mixed:3]"

[[test.results]]
value = "[shape_mixed:1, shape_mixed:2, shape_mixed:3, shape_mixed:arr]"

[[test.results]]
value = "[shape_mixed:1, shape_mixed:2, shape_mixed:3, shape_mixed:arr]"

[[test.results]]
value = "[shape_mixed:3, shape_mixed:arr]"

[[test.results]]
value = "[shape_mixed:3, shape_mixed:arr]"

[[test.results]]
value = "[shape_plain:3, shape_plain:2]"

[[test.results]]
value = "[shape_plain:3, shape_plain:2]"

*/
-- The array record leads a descending order over its whole value.
SELECT VALUE id FROM shape_mixed ORDER BY time DESC LIMIT 2;
SELECT VALUE id FROM shape_mixed WITH NOINDEX ORDER BY time DESC LIMIT 2;
-- And trails an ascending one.
SELECT VALUE id FROM shape_mixed ORDER BY time ASC LIMIT 4;
SELECT VALUE id FROM shape_mixed WITH NOINDEX ORDER BY time ASC LIMIT 4;
-- A range over the band arrays sort in admits the array record.
SELECT VALUE id FROM shape_mixed WHERE time > d'2024-01-02T00:00:00Z' ORDER BY id;
SELECT VALUE id FROM shape_mixed WITH NOINDEX WHERE time > d'2024-01-02T00:00:00Z' ORDER BY id;
-- The index that never held an array orders the same way as a table scan.
SELECT VALUE id FROM shape_plain ORDER BY time DESC LIMIT 2;
SELECT VALUE id FROM shape_plain WITH NOINDEX ORDER BY time DESC LIMIT 2;
