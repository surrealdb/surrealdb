/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = """
	Plan companion to inline_call_in_from: a call in `FROM` inlined
	to a record id plans as a record read by id, locked under `FOR UPDATE`,
	rather than as a source evaluated at run time. A call the rules cannot
	inline (a body of more than one statement) stays a source evaluated at
	run time.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'Project [ctx: Db]
    RecordIdScan [ctx: Db] [record_id: item:one, for_update: true]
''''

[[test.results]]
value = '''
'Project [ctx: Db]
    Limit [ctx: Db] [limit: 0]
        RecordIdScan [ctx: Db] [record_id: item:one, for_update: true]
''''

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    DynamicScan [ctx: Db] [source: fn::dynamic_rid(), limit: 0, for_update: true]
''''
*/

DEFINE FUNCTION fn::rid() { RETURN item:one };
EXPLAIN SELECT * FROM fn::rid() FOR UPDATE;
EXPLAIN SELECT * FROM fn::rid() LIMIT 0 FOR UPDATE;
DEFINE FUNCTION fn::dynamic_rid() { LET $rid = item:one; RETURN $rid; };
EXPLAIN SELECT * FROM fn::dynamic_rid() LIMIT 0 FOR UPDATE;
