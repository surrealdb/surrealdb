/**
[env]
namespace = true
database = true
auth = { level = "owner" }

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
    Filter [ctx: Db] [predicate: k INSIDE (subquery)]
        TableScan [ctx: Db] [table: t, direction: Forward]
        Subquery [ctx: Db] [strategy: once]
            ProjectValue [ctx: Db] [expr: id]
                TableScan [ctx: Db] [table: team, direction: Forward, predicate: ok = true, pre_decode_filter: yes]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: k INSIDE (subquery)]
        TableScan [ctx: Db] [table: t, direction: Forward]
        Subquery [ctx: Db] [strategy: once]
            ProjectValue [ctx: Db] [expr: id]
                Filter [ctx: Db] [predicate: IF id = (team:b) THEN <int> bad ELSE true END]
                    TableScan [ctx: Db] [table: team, direction: Forward]
''''

*/
DEFINE TABLE team SCHEMALESS;
DEFINE TABLE t SCHEMALESS;
EXPLAIN SELECT VALUE id FROM t WHERE k IN (SELECT VALUE id FROM team WHERE ok = true);
EXPLAIN SELECT VALUE id FROM t WHERE k IN (SELECT VALUE id FROM team WHERE IF id = team:b THEN <int> bad ELSE true END);
