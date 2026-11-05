/**
[env]
imports = ["reproductions/layers_guarded_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	Plan companion to layers_scopes: a guarded field read from a
	block, an IF, `$parent` at the statement's own level or inside a nested
	SELECT, a WHERE, a user-defined function call, a closure or a projection
	function naming the field at run time is served by the guard layer above
	the scan, so the layer is present for every one of those scopes; a
	predicate reading the guarded field, by name or through a call evaluated
	against the row, keeps the filter above the layers. The same stack stands
	above a record-id range scan and above an edge-table scan. The plan
	renders a layer without its columns and a nested statement as part of the
	expression that holds it, so the depth of the stack and the placement of
	the filter are what is pinned.
"""

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: s]
    ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
        ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: s]
    ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
        ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: s]
    ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
        ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: s]
    ProjectLayer [ctx: Db] [columns: secret = <permission> @permission, c = <permission> : int @permission]
        ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: row]
    ProjectLayer [ctx: Db] [columns: secret = <permission> @permission, c = <permission> : int @permission]
        ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: $parent.secret = 'hidden']
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
                TableScan [ctx: Db] [table: t, direction: Forward]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: secret = 'hidden']
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
                TableScan [ctx: Db] [table: t, direction: Forward]
"'''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: s]
    ProjectLayer [ctx: Db] [columns: secret = <permission> @permission, c = <permission> : int @permission]
        ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: s]
    ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
        ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: s]
    ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
        ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: owner_flag]
    ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: is_owner, s]
    ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
        ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: s]
    ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
        ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
            RecordIdScan [ctx: Db] [record_id: t:1..=9]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: s]
    ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
        TableScan [ctx: Db] [table: owns, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: s]
    ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
        TableScan [ctx: Db] [table: owns, direction: Forward]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: fn::peek() = 'hidden']
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission, c = <permission> : int @permission]
            ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
                TableScan [ctx: Db] [table: t, direction: Forward]
"'''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: s]
    ProjectLayer [ctx: Db] [columns: secret = <permission> @permission, c = <permission> : int @permission]
        ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: type::field((IF n = 21 THEN (
  Sequence
    Eval['secret']
) ELSE (
  Sequence
    Eval['n']
) END)) = 'hidden']
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission, c = <permission> : int @permission]
            ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
                TableScan [ctx: Db] [table: t, direction: Forward]
"'''
*/

-- A block reading the field.
EXPLAIN SELECT { RETURN secret } AS s FROM t;

-- An IF reading the field.
EXPLAIN SELECT IF true { secret } AS s FROM t;

-- `$parent` at the statement's own level.
EXPLAIN SELECT $parent.secret AS s FROM t;

-- `$parent` from a nested SELECT, as a path and as the whole row.
EXPLAIN SELECT (SELECT VALUE $parent.secret FROM ONLY t:1) AS s FROM t;
EXPLAIN SELECT (SELECT VALUE $parent FROM ONLY t:1) AS row FROM t;

-- The WHERE clause as an oracle.
EXPLAIN SELECT id FROM t WHERE $parent.secret = 'hidden';
EXPLAIN SELECT id FROM t WHERE secret = 'hidden';

-- A user-defined function reading the field off the row.
EXPLAIN SELECT fn::peek() AS s FROM t;

-- A closure, through `$parent` and through the bare field name.
EXPLAIN SELECT array::any([1], |$x| $parent.secret = 'hidden') AS s FROM t;
EXPLAIN SELECT array::any([1], |$x| secret = 'hidden') AS s FROM t;

-- The computed field the guard reads is visible in the same scopes.
EXPLAIN SELECT { RETURN is_owner } AS owner_flag FROM t;
EXPLAIN SELECT is_owner, secret AS s FROM t;

-- A record-id range and an edge table.
EXPLAIN SELECT { RETURN secret } AS s FROM t:1..=9;
EXPLAIN SELECT { RETURN secret } AS s FROM owns;
EXPLAIN SELECT secret AS s FROM owns;

-- The WHERE clause as an oracle through a user-defined function.
EXPLAIN SELECT id FROM t WHERE fn::peek() = 'hidden';

-- A projection function naming the field at run time, projected and as an oracle.
EXPLAIN SELECT type::field(IF n = 21 { 'secret' } ELSE { 'n' }) AS s FROM t;
EXPLAIN SELECT id FROM t WHERE type::field(IF n = 21 { 'secret' } ELSE { 'n' }) = 'hidden';
