/**
[env]
imports = ["reproductions/layers_limit_pushdown_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }
planner-strategy = ["all-ro"]

[test]
reason = """
	Beneath a stack of projection layers the scan takes the pipeline's LIMIT
	and START as it does under a plain table: a layer drops no row, so with
	nothing of the pipeline's own between the layers and the paging — every
	user conjunct moved beneath the layers, or none — the scan pages and the
	Limit node goes. A conjunct on a computed column stays above the layer
	and keeps the paging above it, as does an ORDER BY on one: the sort runs
	above the layer over every row, so the scan carries no paging beneath
	it. An ORDER BY the scan itself satisfies passes through the layers with
	the paging. Beneath a table permission nothing is pushed: the permission
	drops rows the scan would have counted, so the Limit stays above the
	PermissionFilter and the scan carries no paging.
"""

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: dbl]
    ProjectLayer [ctx: Db] [columns: dbl = <computed> : int @computed]
        TableScan [ctx: Db] [table: t, direction: Forward, limit: 2, offset: 1]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: dbl]
    ProjectLayer [ctx: Db] [columns: dbl = <computed> : int @computed]
        TableScan [ctx: Db] [table: t, direction: Forward, predicate: n > 1, limit: 2, offset: 1, pre_decode_filter: yes]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: dbl]
    Limit [ctx: Db] [limit: 2, offset: 1]
        Filter [ctx: Db] [predicate: dbl > 2]
            ProjectLayer [ctx: Db] [columns: dbl = <computed> : int @computed]
                TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: dbl]
    Limit [ctx: Db] [limit: 2]
        SortTopKByKey [ctx: Db] [sort_keys: dbl DESC, limit: 2]
            ProjectLayer [ctx: Db] [columns: dbl = <computed> : int @computed]
                TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: dbl]
    ProjectLayer [ctx: Db] [columns: dbl = <computed> : int @computed]
        TableScan [ctx: Db] [table: t, direction: Backward, limit: 2, offset: 1]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 1]
        PermissionFilter [ctx: Db] [predicate: <permission>]
            TableScan [ctx: Db] [table: gated, direction: Forward]
''''
*/

EXPLAIN SELECT dbl FROM t LIMIT 2 START 1;
EXPLAIN SELECT dbl FROM t WHERE n > 1 LIMIT 2 START 1;
EXPLAIN SELECT dbl FROM t WHERE dbl > 2 LIMIT 2 START 1;
EXPLAIN SELECT dbl FROM t ORDER BY dbl DESC LIMIT 2;
EXPLAIN SELECT dbl FROM t ORDER BY id DESC LIMIT 2 START 1;
EXPLAIN SELECT id FROM gated LIMIT 1;
