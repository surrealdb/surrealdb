/**
[env]
imports = ["reproductions/layers_fold_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }
planner-strategy = ["all-ro"]

[test]
reason = """
	Beneath a stack of projection layers the access path is chosen as it is
	beneath a table permission. The conjuncts whose field no layer names move
	beneath the stack: the scan applies them, so a row they reject is never
	decoded or layered, and an indexed one selects its index and is answered
	by it in full. A conjunct on a layer column — a computed field or a
	guarded field — stays above the stack, since beneath it the field is the
	stored value or absent, and is judged over the row the stack presents;
	LIMIT and START stay above that filter. With no such filter the scan
	takes the paging, as it does under a plain table: the layers drop no
	row.
"""

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 10, offset: 1]
        Filter [ctx: Db] [predicate: dbl > 2]
            ProjectLayer [ctx: Db] [columns: dbl = <computed> : int @computed]
                TableScan [ctx: Db] [table: t, direction: Forward, predicate: st INSIDE [200, 201], pre_decode_filter: yes]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: secret = 's1']
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            TableScan [ctx: Db] [table: t, direction: Forward, predicate: st = 200, pre_decode_filter: yes]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id, dbl]
    Filter [ctx: Db] [predicate: dbl > 2]
        ProjectLayer [ctx: Db] [columns: dbl = <computed> : int @computed]
            IndexScan [ctx: Db] [index: idx_cat, access: = 'a', direction: Forward, predicate: cat = 'a']
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: dbl > 2 AND secret = 's1']
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            ProjectLayer [ctx: Db] [columns: dbl = <computed> : int @computed]
                TableScan [ctx: Db] [table: t, direction: Forward]
"'''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: dbl]
    ProjectLayer [ctx: Db] [columns: dbl = <computed> : int @computed]
        TableScan [ctx: Db] [table: t, direction: Forward, limit: 10, offset: 1]
''''

*/

-- A passthrough conjunct beside one on a computed column, over a table scan:
-- the scan applies the passthrough, the filter above the stack judges the
-- other, and paging stays above the filter.
EXPLAIN SELECT id FROM t WHERE st IN [200, 201] AND dbl > 2 LIMIT 10 START 1;

-- A passthrough conjunct beside one on the guarded column.
EXPLAIN SELECT id FROM t WHERE st = 200 AND secret = 's1';

-- An indexed passthrough conjunct drives the scan beneath the stack.
EXPLAIN SELECT id, dbl FROM t WHERE cat = 'a' AND dbl > 2;

-- Only layer columns: nothing reaches the scan.
EXPLAIN SELECT id FROM t WHERE dbl > 2 AND secret = 's1';

-- No predicate: the layers derive what the projection reads, and the scan
-- pages.
EXPLAIN SELECT dbl FROM t LIMIT 10 START 1;
