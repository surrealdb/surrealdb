/**
[env]
namespace = true
database = true
imports = ["language/statements/define/table/permission_subquery_join_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }
planner-strategy = ["all-ro"]

[test]
reason = """
	Plan companion to permission_subquery_join: for the record user the
	permission's subquery is a hash join in the permission frame whose keys,
	residual and right side are the table definer's and are hidden from the
	plan, as a permission filter's predicate is; the caller's own predicate
	is judged above the join.
"""

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    HashJoin [ctx: Db] [type: Semi, alias: _join0, on: <permission>, frame: permission]
        TableScan [ctx: Db] [table: doc, direction: Forward]
''''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: title != 'one']
        HashJoin [ctx: Db] [type: Semi, alias: _join0, on: <permission>, frame: permission]
            TableScan [ctx: Db] [table: doc, direction: Forward]
"'''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    HashJoin [ctx: Db] [type: Semi, alias: _join0, on: <permission>, frame: permission]
        TableScan [ctx: Db] [table: note, direction: Forward]
''''
*/

EXPLAIN SELECT VALUE id FROM doc;
EXPLAIN SELECT VALUE id FROM doc WHERE title != 'one';
EXPLAIN SELECT VALUE id FROM note;
