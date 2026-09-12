/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro"]

[test]
reason = """
	A record id whose key is generated names a different record each time it
	is built, so it is not pure and decorrelation may not lift a subquery
	whose right side holds one. A written-down key still is pure and still
	lifts. The inline rule reaches the same answer through `may_raise`, which
	a generated key already fails.
"""

[[test.results]]
value = "[{ id: o:1, k: 1 }]"

[[test.results]]
value = "[{ id: r:1, k: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: k]
    TableScan [ctx: Db] [table: o, direction: Forward, predicate: k INSIDE (ProjectValue), pre_decode_filter: no (unsupported predicate)]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: k]
    TableScan [ctx: Db] [table: o, direction: Forward, predicate: k INSIDE (ProjectValue), pre_decode_filter: no (unsupported predicate)]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: k]
    HashJoin [ctx: Db] [type: Semi, alias: _join0, on: k = _join0.k]
        TableScan [ctx: Db] [table: o, direction: Forward]
        RecordIdScan [ctx: Db] [record_id: r:1, predicate: r:1 = id, pre_decode_filter: yes]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: fn::twice(a:rand())]
    TableScan [ctx: Db] [table: o, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: [a:1, a:1]]
    TableScan [ctx: Db] [table: o, direction: Forward]
''''
*/

-- `t:rand()`, `t:ulid()` and `t:uuid()` produce their key where the id is
-- built. `rid.key` is what says so; no expression walk reaches it, since a
-- generated key carries no sub-expression.

CREATE o:1 SET k = 1;
CREATE r:1 SET k = 1;
DEFINE FUNCTION fn::twice($x: any) { RETURN [$x, $x] };

-- The lift builds the right relation once for every outer row to probe; a
-- generated id in its filter or its `VALUE` answers differently each time.
EXPLAIN SELECT VALUE k FROM o WHERE k IN (SELECT VALUE k FROM r WHERE string::slice(<string> x:rand(), 2, 3) < 'N');
EXPLAIN SELECT VALUE k FROM o WHERE k IN (SELECT VALUE k FROM r WHERE <string> x:ulid() < 'x:z');

-- A key the statement writes down is the same record every time, so the
-- same shape still lifts.
EXPLAIN SELECT VALUE k FROM o WHERE k IN (SELECT VALUE k FROM r WHERE r:1 = id);

-- Inlining substitutes the argument for every mention of the parameter, so
-- a generated id would become two independent records. `may_raise` already
-- keeps the call; these pin that it stays kept.
EXPLAIN SELECT VALUE fn::twice(a:rand()) FROM o;
EXPLAIN SELECT VALUE fn::twice(a:1) FROM o;
