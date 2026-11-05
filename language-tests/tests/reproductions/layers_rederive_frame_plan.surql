/**
[env]
imports = ["reproductions/layers_rederive_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	Plan companion to layers_rederive_frame: a computed field that
	dereferences its own record or a linked record is re-derived by a layer
	above the scan, and a guarded computed field adds its guard layer above
	the layer that derives it; `SELECT *` stacks every layer the table's
	fields need; a predicate on a re-derived field keeps the filter above the
	layer, so the scan carries no predicate of its own.
"""

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: self_g, self_g_open, link_hidden, link_hidden_open]
    ProjectLayer [ctx: Db] [columns: g = <permission> @permission, link_hidden = <permission> : none | string @permission, self_g = <permission> : none | string @permission, self_g_open = <computed> : none | string @computed]
        ProjectLayer [ctx: Db] [columns: link_hidden = <computed> : none | string @computed, link_hidden_open = <computed> : none | string @computed]
            TableScan [ctx: Db] [table: item, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: g_len]
    ProjectLayer [ctx: Db] [columns: g = <permission> @permission, g_len = <permission> : int @permission]
        TableScan [ctx: Db] [table: item, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    ProjectLayer [ctx: Db] [columns: g = <permission> @permission, g_len = <permission> : int @permission, link_hidden = <permission> : none | string @permission, self_g = <permission> : none | string @permission, self_g_open = <computed> : none | string @computed]
        ProjectLayer [ctx: Db] [columns: link_hidden = <computed> : none | string @computed, link_hidden_open = <computed> : none | string @computed]
            TableScan [ctx: Db] [table: item, direction: Forward]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: self_g = 'x']
        ProjectLayer [ctx: Db] [columns: g = <permission> @permission, self_g = <permission> : none | string @permission]
            TableScan [ctx: Db] [table: item, direction: Forward]
"'''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: self_g = NONE]
        ProjectLayer [ctx: Db] [columns: g = <permission> @permission, self_g = <permission> : none | string @permission]
            TableScan [ctx: Db] [table: item, direction: Forward]
''''
*/

EXPLAIN SELECT self_g, self_g_open, link_hidden, link_hidden_open FROM item;
EXPLAIN SELECT g_len FROM item;
EXPLAIN SELECT * FROM item;
EXPLAIN SELECT id FROM item WHERE self_g = 'x';
EXPLAIN SELECT id FROM item WHERE self_g = NONE;
