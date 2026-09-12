/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = """
	Plan companion to projection_names: an unaliased projection whose
	expression is a statement in scalar position (a nested SELECT, an IF, a
	block, a RETURN, or a path starting from one) is keyed by the SurrealQL
	text of the expression as written, and that key is what the projection
	node carries; an aliased nested SELECT is keyed by its alias, and a
	SELECT VALUE carries the nested statement as its single value. The nested
	statement's own plan appears beneath the outer projection.
"""

[[test.results]]
value = "[{ id: a:1 }]"

[[test.results]]
value = "[{ id: b:1 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: SELECT * FROM b]
    Compute [ctx: Db] [fields: SELECT * FROM b = (SelectProject)]
      SELECT * FROM b.subquery: SelectProject [ctx: Db] [projections: *]
          TableScan [ctx: Db] [table: b, direction: Forward]
        TableScan [ctx: Db] [table: a, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: IF true { 1 } ELSE { 2 }]
    Compute [ctx: Db] [fields: IF true { 1 } ELSE { 2 } = IF true THEN (Expr) ELSE (Expr) END]
        TableScan [ctx: Db] [table: a, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: { RETURN 1 }]
    Compute [ctx: Db] [fields: { RETURN 1 } = (Return)]
      { RETURN 1 }.subquery: Return [ctx: Rt]
          Expr [ctx: Rt] [expr: 1]
        TableScan [ctx: Db] [table: a, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: RETURN 1]
    Compute [ctx: Db] [fields: RETURN 1 = (Return)]
      RETURN 1.subquery: Return [ctx: Rt]
          Expr [ctx: Rt] [expr: 1]
        TableScan [ctx: Db] [table: a, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: (SELECT VALUE id FROM b)[0]]
    Compute [ctx: Db] [fields: (SELECT VALUE id FROM b)[0] = (
  Project[VALUE id]
    Source[b]
)[0]]
      (SELECT VALUE id FROM b)[0].subquery: ProjectValue [ctx: Db] [expr: id]
          TableScan [ctx: Db] [table: b, direction: Forward]
        TableScan [ctx: Db] [table: a, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: (SELECT * FROM b).id]
    Compute [ctx: Db] [fields: (SELECT * FROM b).id = (
  Project[*]
    Source[b]
).id]
      (SELECT * FROM b).id.subquery: SelectProject [ctx: Db] [projections: *]
          TableScan [ctx: Db] [table: b, direction: Forward]
        TableScan [ctx: Db] [table: a, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: IF true THEN 1 ELSE 2 END]
    Compute [ctx: Db] [fields: IF true THEN 1 ELSE 2 END = IF true THEN 1 ELSE 2 END]
        TableScan [ctx: Db] [table: a, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: (SELECT * FROM ONLY b LIMIT 1).id]
    Compute [ctx: Db] [fields: (SELECT * FROM ONLY b LIMIT 1).id = (
  Only
    Project[*]
      Limit[LIMIT 1]
        Source[b]
).id]
      (SELECT * FROM ONLY b LIMIT 1).id.subquery: UnwrapExactlyOne [ctx: Db]
          SelectProject [ctx: Db] [projections: *]
              TableScan [ctx: Db] [table: b, direction: Forward, limit: 1]
        TableScan [ctx: Db] [table: a, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: sub]
    Compute [ctx: Db] [fields: sub = (SelectProject)]
      sub.subquery: SelectProject [ctx: Db] [projections: *]
          TableScan [ctx: Db] [table: b, direction: Forward]
        TableScan [ctx: Db] [table: a, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: (SelectProject)]
  expr.subquery: SelectProject [ctx: Db] [projections: *]
      TableScan [ctx: Db] [table: b, direction: Forward]
    TableScan [ctx: Db] [table: a, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: [(SELECT * FROM b)]]
    Compute [ctx: Db] [fields: [(SELECT * FROM b)] = [(SelectProject)]]
        TableScan [ctx: Db] [table: a, direction: Forward]
''''
*/

CREATE a:1; CREATE b:1;
EXPLAIN SELECT (SELECT * FROM b) FROM a;
EXPLAIN SELECT IF true { 1 } ELSE { 2 } FROM a;
EXPLAIN SELECT { RETURN 1 } FROM a;
EXPLAIN SELECT (RETURN 1) FROM a;
EXPLAIN SELECT (SELECT VALUE id FROM b)[0] FROM a;
EXPLAIN SELECT (SELECT * FROM b).id FROM a;
EXPLAIN SELECT IF true THEN 1 ELSE 2 END FROM a;
EXPLAIN SELECT (SELECT * FROM ONLY b LIMIT 1).id FROM a;
EXPLAIN SELECT (SELECT * FROM b) AS sub FROM a;
EXPLAIN SELECT VALUE (SELECT * FROM b) FROM a;
EXPLAIN SELECT [(SELECT * FROM b)] FROM a;
