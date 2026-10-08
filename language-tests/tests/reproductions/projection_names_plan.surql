/**
[env]

[test]
reason = """
	Plan companion to projection_names: an unaliased projection whose
	expression is a statement in scalar position (a nested SELECT, an IF, a
	block, a RETURN, or a path starting from one) is keyed by the SurrealQL
	text of the expression as written, and that key is what the projection
	node carries; an aliased nested SELECT is keyed by its alias, and a
	SELECT VALUE carries the nested statement as its single value. The plan
	renders the nested statement within the expression of the projection that
	holds it.
"""

[[test.results]]
value = "[{ id: a:1 }]"

[[test.results]]
value = "[{ id: b:1 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: SELECT * FROM b]
    TableScan [ctx: Db] [table: a, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: IF true { 1 } ELSE { 2 }]
    TableScan [ctx: Db] [table: a, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: { RETURN 1 }]
    TableScan [ctx: Db] [table: a, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: RETURN 1]
    TableScan [ctx: Db] [table: a, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: (SELECT VALUE id FROM b)[0]]
    TableScan [ctx: Db] [table: a, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: (SELECT * FROM b).id]
    TableScan [ctx: Db] [table: a, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: IF true THEN 1 ELSE 2 END]
    TableScan [ctx: Db] [table: a, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: (SELECT * FROM ONLY b LIMIT 1).id]
    TableScan [ctx: Db] [table: a, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: sub]
    TableScan [ctx: Db] [table: a, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: (
  Project[*]
    Source[b]
)]
    TableScan [ctx: Db] [table: a, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: [(SELECT * FROM b)]]
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
