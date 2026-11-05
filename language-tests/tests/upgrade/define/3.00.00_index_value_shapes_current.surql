/**
[env]
# The body asserts `EXPLAIN` plans, and the ordinary language-test runner
# replays it as well as the upgrade axis.
imports = ["upgrade/define/3.00.00_index_value_shapes_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
reason = """
An index whose column has stored no array plans that column as a scalar one: a
backward scan of the index answers ORDER BY ... DESC LIMIT on its own, with no
sort above it. That needs the index to have recorded every value shape it
stored, which only a build on this checkout does. An index an older binary
built recorded none, so it plans as a column that may hold an array, with the
sort kept, until it is rebuilt; the rows are the same either way and are
asserted on every hop in 3.00.00_index_value_shapes.surql.

The plan cannot span the 3.x line, so it lives here, gated to the current
checkout. The ordinary runner replays this file with the import written by this
checkout, where the index has recorded its shapes from the start."""

[[test.repair]]
since = ">=3.4.0-a"
script = "upgrade/define/3.00.00_index_value_shapes_rebuild.surql"
unrepaired = "degraded"
reason = """
An index built before indexes recorded the value shapes they store may hold an
array in any column without a declared kind, so ORDER BY over such a column
sorts every row the scan reads rather than reading the first entries of the
index. `REBUILD INDEX` starts the index recording its shapes, and a column that
never held an array then plans as a scalar column."""

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [table: shape_plain, index: shape_plain_time, direction: Backward, limit: 2]
''''

[[test.results]]
value = "[shape_plain:3, shape_plain:2]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 2]
        Sort [ctx: Db] [sort_keys: time DESC, keep: 2]
            TableScan [ctx: Db] [table: shape_mixed, direction: Forward, topk_pushdown: yes]
''''

[[test.results]]
value = "[shape_mixed:arr, shape_mixed:3]"

*/
-- A column that never held an array: the index answers the order.
EXPLAIN SELECT id FROM shape_plain ORDER BY time DESC LIMIT 2;
SELECT VALUE id FROM shape_plain ORDER BY time DESC LIMIT 2;
-- A column that held an array keeps the sort, rebuilt or not.
EXPLAIN SELECT id FROM shape_mixed ORDER BY time DESC LIMIT 2;
SELECT VALUE id FROM shape_mixed ORDER BY time DESC LIMIT 2;
