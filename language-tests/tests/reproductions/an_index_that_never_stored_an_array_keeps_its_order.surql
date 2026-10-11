/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
issue = 1587
reason = """
A plain index column with no declared kind plans as a scalar column for as long
as its index has stored no array in it: the scan delivers the column's order,
so ORDER BY ... LIMIT reads the first entries of the index and needs no sort.
The first array written to the column changes the plan in every later
statement, a function's cached body and a statement of the same transaction
included, to the plan of a column that may hold an array, whose answer is the
whole-value order. A statement that writes is planned before its writes run,
so it never plans a column as scalar on what the index has recorded: a block
that writes an array and then reads answers the whole-value order too.
Deleting the array leaves the plan as it is; REBUILD INDEX clears the index and
plans it as a scalar column again. Every indexed read answers what its WITH
NOINDEX twin answers."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: s:1, time: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "[{ id: s:2, time: d'2024-01-02T00:00:00Z' }]"

[[test.results]]
value = "[{ id: s:3, time: d'2024-01-03T00:00:00Z' }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [table: s, index: s_time, direction: Backward, limit: 2]
''''

[[test.results]]
value = "[s:3, s:2]"

[[test.results]]
value = "[s:3, s:2]"

[[test.results]]
value = "[{ id: s:4, time: [d'2023-01-01T00:00:00Z'] }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 2]
        Sort [ctx: Db] [sort_keys: time DESC, keep: 2]
            TableScan [ctx: Db] [table: s, direction: Forward, topk_pushdown: yes]
''''

[[test.results]]
value = "[s:4, s:3]"

[[test.results]]
value = "[s:4, s:3]"

[[test.results]]
value = "[s:4, s:3]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 2]
        Sort [ctx: Db] [sort_keys: time DESC, keep: 2]
            TableScan [ctx: Db] [table: s, direction: Forward, topk_pushdown: yes]
''''

[[test.results]]
value = "[s:3, s:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [table: s, index: s_time, direction: Backward, limit: 2]
''''

[[test.results]]
value = "[s:3, s:2]"

[[test.results]]
value = "[s:3, s:2]"

[[test.results]]
value = "[{ id: s:5, time: [] }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 2]
        Sort [ctx: Db] [sort_keys: time DESC, keep: 2]
            TableScan [ctx: Db] [table: s, direction: Forward, topk_pushdown: yes]
''''

[[test.results]]
value = "[s:5, s:3]"

[[test.results]]
value = "[s:5, s:3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: s:6, time: [d'2022-01-01T00:00:00Z'] }]"

[[test.results]]
value = "[s:6, s:3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[s:7, s:3]"

*/

DEFINE TABLE s SCHEMALESS;
DEFINE INDEX s_time ON s FIELDS time;
DEFINE FUNCTION fn::latest() { RETURN SELECT VALUE id FROM s ORDER BY time DESC LIMIT 2 };
CREATE s:1 SET time = d'2024-01-01';
CREATE s:2 SET time = d'2024-01-02';
CREATE s:3 SET time = d'2024-01-03';

-- No array stored: the backward index scan answers the order.
EXPLAIN SELECT id FROM s ORDER BY time DESC LIMIT 2;
SELECT VALUE id FROM s ORDER BY time DESC LIMIT 2;
fn::latest();

-- An array sorts after every datetime, but its entries sit at its elements.
CREATE s:4 SET time = [d'2023-01-01'];
EXPLAIN SELECT id FROM s ORDER BY time DESC LIMIT 2;
SELECT VALUE id FROM s ORDER BY time DESC LIMIT 2;
SELECT VALUE id FROM s WITH NOINDEX ORDER BY time DESC LIMIT 2;
fn::latest();

-- The array is gone, but the index still records storing one.
DELETE s:4;
EXPLAIN SELECT id FROM s ORDER BY time DESC LIMIT 2;
SELECT VALUE id FROM s ORDER BY time DESC LIMIT 2;

-- A rebuild starts the index over.
REBUILD INDEX s_time ON s;
EXPLAIN SELECT id FROM s ORDER BY time DESC LIMIT 2;
SELECT VALUE id FROM s ORDER BY time DESC LIMIT 2;
fn::latest();

-- An empty array is stored under NULL, and counts as an array.
CREATE s:5 SET time = [];
EXPLAIN SELECT id FROM s ORDER BY time DESC LIMIT 2;
SELECT VALUE id FROM s ORDER BY time DESC LIMIT 2;
SELECT VALUE id FROM s WITH NOINDEX ORDER BY time DESC LIMIT 2;
DELETE s:5;
REBUILD INDEX s_time ON s;

-- A statement after the write in the same transaction plans on the array.
BEGIN;
CREATE s:6 SET time = [d'2022-01-01'];
SELECT VALUE id FROM s ORDER BY time DESC LIMIT 2;
COMMIT;
DELETE s:6;
REBUILD INDEX s_time ON s;

-- So does a statement after the write in the same block.
{
	CREATE s:7 SET time = [d'2021-01-01'];
	RETURN SELECT VALUE id FROM s ORDER BY time DESC LIMIT 2;
};
