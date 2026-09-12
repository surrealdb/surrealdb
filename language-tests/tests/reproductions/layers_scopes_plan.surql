/**
[env]
imports = ["reproductions/layers_guarded_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }
planner-strategy = ["all-ro"]

[test]
reason = """
	Plan companion to layers_scopes: a guarded field read from a
	block, an IF, `$parent` at the statement's own level or inside a nested
	SELECT, a WHERE, a user-defined function call, a closure or a projection
	function naming the field at run time is served by the guard layer above
	the scan, so the layer is present for every one of those scopes and the
	user expression sits above it; a predicate reading the guarded field, by
	name or through a call evaluated against the row, keeps the filter above
	the layer. The same stack stands above a record-id range scan and above
	an edge-table scan.
"""

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: s]
    Compute [ctx: Db] [fields: s = (Return)]
      s.subquery: Return [ctx: Db]
          Expr [ctx: Db] [expr: secret]
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
                TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: s]
    Compute [ctx: Db] [fields: s = IF true THEN (Expr) END]
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
                TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: s]
    Compute [ctx: Db] [fields: s = $parent.secret]
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
                TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: s]
    Compute [ctx: Db] [fields: s = (UnwrapExactlyOne)]
      s.subquery: UnwrapExactlyOne [ctx: Db]
          ProjectValue [ctx: Db] [expr: $parent.secret]
              ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
                  ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
                      Project [ctx: Db]
                          RecordIdScan [ctx: Db] [record_id: t:1]
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission, c = <permission> : int @permission]
            ProjectLayer [ctx: Db] [columns: c = <computed> : int @computed, is_owner = <computed> : bool @computed]
                TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: row]
    Compute [ctx: Db] [fields: row = (UnwrapExactlyOne)]
      row.subquery: UnwrapExactlyOne [ctx: Db]
          ProjectValue [ctx: Db] [expr: $parent]
              ProjectLayer [ctx: Db] [columns: secret = <permission> @permission, c = <permission> : int @permission]
                  ProjectLayer [ctx: Db] [columns: c = <computed> : int @computed, is_owner = <computed> : bool @computed]
                      Project [ctx: Db]
                          RecordIdScan [ctx: Db] [record_id: t:1]
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission, c = <permission> : int @permission]
            ProjectLayer [ctx: Db] [columns: c = <computed> : int @computed, is_owner = <computed> : bool @computed]
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
    Compute [ctx: Db] [fields: s = fn::peek()]
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission, c = <permission> : int @permission]
            ProjectLayer [ctx: Db] [columns: c = <computed> : int @computed, is_owner = <computed> : bool @computed]
                TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: s]
    Compute [ctx: Db] [fields: s = array::any([1], |$x: any| ($parent.secret = 'hidden'))]
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
                TableScan [ctx: Db] [table: t, direction: Forward]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: s]
    Compute [ctx: Db] [fields: s = array::any([1], |$x: any| (secret = 'hidden'))]
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
                TableScan [ctx: Db] [table: t, direction: Forward]
"'''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: owner_flag]
    Compute [ctx: Db] [fields: owner_flag = (Return)]
      owner_flag.subquery: Return [ctx: Db]
          Expr [ctx: Db] [expr: is_owner]
        ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: is_owner, secret AS s]
    ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
        ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: s]
    Compute [ctx: Db] [fields: s = (Return)]
      s.subquery: Return [ctx: Db]
          Expr [ctx: Db] [expr: secret]
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            ProjectLayer [ctx: Db] [columns: is_owner = <computed> : bool @computed]
                Project [ctx: Db]
                    RecordIdScan [ctx: Db] [record_id: t:1..=9]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: s]
    Compute [ctx: Db] [fields: s = (Return)]
      s.subquery: Return [ctx: Db]
          Expr [ctx: Db] [expr: secret]
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
            TableScan [ctx: Db] [table: owns, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: secret AS s]
    ProjectLayer [ctx: Db] [columns: secret = <permission> @permission]
        TableScan [ctx: Db] [table: owns, direction: Forward]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: fn::peek() = 'hidden']
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission, c = <permission> : int @permission]
            ProjectLayer [ctx: Db] [columns: c = <computed> : int @computed, is_owner = <computed> : bool @computed]
                TableScan [ctx: Db] [table: t, direction: Forward]
"'''

[[test.results]]
value = '''
'Project [ctx: Db]
    ProjectLayer [ctx: Db] [columns: secret = <permission> @permission, c = <permission> : int @permission]
        ProjectLayer [ctx: Db] [columns: c = <computed> : int @computed, is_owner = <computed> : bool @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: type::field(IF n = 21 THEN (Expr) ELSE (Expr) END) = 'hidden']
        ProjectLayer [ctx: Db] [columns: secret = <permission> @permission, c = <permission> : int @permission]
            ProjectLayer [ctx: Db] [columns: c = <computed> : int @computed, is_owner = <computed> : bool @computed]
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
