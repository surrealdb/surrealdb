/**
[env]
imports = ["reproductions/layers_collision_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	Plan companion to layers_name_collision: each computed field and
	each guarded field lands in the layer stack under its own name, so `a1`
	and `a`, `_a1` and `_a10` appear as separate layer columns: a projection
	naming both a computed field and a denied one derives the former and
	guards the latter, `SELECT *` derives every computed field and guards
	every guarded one, and a predicate on the computed fields keeps the
	filter above the layers that derive them.
"""

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: first, second]
    ProjectLayer [ctx: Db] [columns: a = <permission> @permission]
        ProjectLayer [ctx: Db] [columns: a1 = <computed> : int @computed]
            TableScan [ctx: Db] [table: u, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: first, second]
    ProjectLayer [ctx: Db] [columns: _a10 = <permission> @permission]
        ProjectLayer [ctx: Db] [columns: _a1 = <computed> : int @computed]
            TableScan [ctx: Db] [table: u, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    ProjectLayer [ctx: Db] [columns: _a10 = <permission> @permission, a = <permission> @permission]
        ProjectLayer [ctx: Db] [columns: _a1 = <computed> : int @computed, a1 = <computed> : int @computed, b = <computed> : int @computed, c = <computed> : int @computed, d = <computed> : int @computed, e = <computed> : int @computed, f = <computed> : int @computed, g = <computed> : int @computed, h = <computed> : int @computed, i = <computed> : int @computed, j = <computed> : int @computed]
            TableScan [ctx: Db] [table: u, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: x, y]
    Filter [ctx: Db] [predicate: a1 = 1 AND _a1 = 11]
        ProjectLayer [ctx: Db] [columns: _a10 = <permission> @permission, a = <permission> @permission]
            ProjectLayer [ctx: Db] [columns: _a1 = <computed> : int @computed, a1 = <computed> : int @computed]
                TableScan [ctx: Db] [table: u, direction: Forward]
''''
*/

EXPLAIN SELECT a1 AS first, a AS second FROM u;
EXPLAIN SELECT _a1 AS first, _a10 AS second FROM u;
EXPLAIN SELECT * FROM u;
EXPLAIN SELECT a AS x, _a10 AS y FROM u WHERE a1 = 1 AND _a1 = 11;
