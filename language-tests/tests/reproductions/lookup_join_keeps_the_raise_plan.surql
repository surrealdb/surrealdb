/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro"]

[test]
reason = """
	An id-keyed lift is planned as a `LookupJoin` — the right table read by a
	batched get of the left keys — only where the chain over the table is
	inert. A predicate that can raise puts the read back on a `HashJoin` over
	the whole relation, which is what the scalar form the lift replaced read.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    LookupJoin [ctx: Db] [type: Semi, alias: _join0, table: team, on: k = _join0.id]
        TableScan [ctx: Db] [table: t, direction: Forward]
        Filter [ctx: Db] [predicate: ok = true]
            RecordBatchFetch [ctx: Db] [table: team]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    HashJoin [ctx: Db] [type: Semi, alias: _join0, on: k = _join0.id]
        TableScan [ctx: Db] [table: t, direction: Forward]
        TableScan [ctx: Db] [table: team, direction: Forward, predicate: IF id = team:b THEN <int>  bad ELSE true END, pre_decode_filter: no (unsupported predicate)]
''''

*/
DEFINE TABLE team SCHEMALESS;
DEFINE TABLE t SCHEMALESS;
EXPLAIN SELECT VALUE id FROM t WHERE k IN (SELECT VALUE id FROM team WHERE ok = true);
EXPLAIN SELECT VALUE id FROM t WHERE k IN (SELECT VALUE id FROM team WHERE IF id = team:b THEN <int> bad ELSE true END);
