/**
[env]
backend = ["mem", "surrealkv"]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	EXPLAIN ANALYZE of a statement after a schema statement whose effect
	depends on the data runs in the database the session names when it runs:
	one the block removed and defined again is read as it is now.
"""

[[test.results]]
value = "[{ id: flag:1, on: true }]"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *] {rows: 2}
    TableScan [ctx: Db] [table: t, direction: Forward] {rows: 2}

Total rows: 2''''

[[test.results]]
value = "[{ id: t:8 }, { id: t:9 }]"
*/
CREATE flag:1 SET on = true;
CREATE t:1;
{ IF (SELECT VALUE on FROM ONLY flag:1) { REMOVE DATABASE test; DEFINE DATABASE test; }; CREATE t:9; CREATE t:8; EXPLAIN ANALYZE SELECT * FROM t };
SELECT * FROM t;
