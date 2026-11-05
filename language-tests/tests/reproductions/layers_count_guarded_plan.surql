/**
[env]
imports = ["reproductions/layers_guarded_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	A field's guard is evaluated only where the field can reach the caller.
	A count that reads no field keeps the key-range count path over a table
	with guarded fields; a count whose predicate reads an unguarded stored
	field derives and guards nothing; one whose predicate reads a guarded
	field brings that field's guard, and only that guard, into the plan.
"""

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    CountScan [ctx: Db] [source: t]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Aggregate [ctx: Db] [mode: GROUP ALL]
        TableScan [ctx: Db] [table: t, direction: Forward, predicate: n > 1, pre_decode_filter: yes]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    Aggregate [ctx: Db] [mode: GROUP ALL]
        Filter [ctx: Db] [predicate: secret = 'hidden']
            ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
                ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
                    TableScan [ctx: Db] [table: t, direction: Forward]
"'''

*/

EXPLAIN SELECT count() FROM t GROUP ALL;
EXPLAIN SELECT count() FROM t WHERE n > 1 GROUP ALL;
EXPLAIN SELECT count() FROM t WHERE secret = 'hidden' GROUP ALL;
