/**
[env]
imports = ["reproductions/layers_guarded_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	Plan companion to layers_param_shadow: the layer stack is built
	from the fields a statement reads, never from the parameters in scope, so
	the plan for a projection of `is_owner`, `secret` and `c` is the same
	before and after parameters named after them are bound; a predicate on
	`is_owner` keeps the filter above the layers that guard and derive it; a
	projection of parameters alone brings no layer. The plan renders a layer
	without its columns, so the depth of the stack is what is pinned.
"""

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: is_owner, s, cc]
    ProjectLayer [ctx: Db] [columns: secret = <permission> @permission, c = <permission> : int @permission]
        ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: is_owner, s, cc]
    ProjectLayer [ctx: Db] [columns: secret = <permission> @permission, c = <permission> : int @permission]
        ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: s]
    Filter [ctx: Db] [predicate: is_owner = true]
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
                TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: a, b, p]
    TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: raw]
    TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: x]
    TableScan [ctx: Db] [table: t, direction: Forward]
''''
*/

-- Before any parameter is set.
EXPLAIN SELECT is_owner, secret AS s, c AS cc FROM t;

-- Parameters named after the guard's input and after the guarded fields.
LET $_is_owner0 = true;
LET $is_owner = true;
LET $_secret1 = 'leak';
LET $_c0 = 42;
EXPLAIN SELECT is_owner, secret AS s, c AS cc FROM t;
EXPLAIN SELECT secret AS s FROM t WHERE is_owner = true;

-- The parameters themselves read as what the caller set, nothing else.
EXPLAIN SELECT $_is_owner0 AS a, $_secret1 AS b, $_c0 AS p FROM t;
EXPLAIN SELECT $_c1 AS raw FROM t;

-- An ordinary underscore-prefixed parameter keeps working.
LET $_x = 5;
EXPLAIN SELECT $_x + n AS x FROM t;
