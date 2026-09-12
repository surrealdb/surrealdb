/**
[env]
imports = ["reproductions/layers_count_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }
planner-strategy = ["all-ro"]

[test]
reason = """
	Plan companion to layers_count_fast_path. A count over a table
	with a computed field keeps the key-range count path when the count reads
	no computed field: no projection layer stands between the aggregate and
	the source. A count whose predicate reads the computed field derives it,
	and the layer appears above the scan.
"""

# 0: no computed field is read: the key-range count, no layer.
[[test.results]]
value = '''
'CountScan [ctx: Db] [source: t]
''''

# 1: a predicate on a stored field: still no layer between the aggregate and the scan.
[[test.results]]
value = '''
'Aggregate [ctx: Db] [mode: GROUP ALL]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: a > 1, pre_decode_filter: yes]
''''

# 2: a predicate on the computed field derives it: the layer stands above the scan.
[[test.results]]
value = '''
'Aggregate [ctx: Db] [mode: GROUP ALL]
    Filter [ctx: Db] [predicate: n > 0]
        ProjectLayer [ctx: Db] [columns: n = <computed> : int @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''
*/

EXPLAIN SELECT count() FROM t GROUP ALL;
EXPLAIN SELECT count() FROM t WHERE a > 1 GROUP ALL;
EXPLAIN SELECT count() FROM t WHERE n > 0 GROUP ALL;
