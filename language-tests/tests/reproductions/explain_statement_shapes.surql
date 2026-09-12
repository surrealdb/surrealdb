/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = """EXPLAIN of a statement that is not a relational pipeline (RETURN, a block, \
LET, IF) but names a table prints the plan of the statement it wraps; the catalog \
resolution of the table happens beneath the statement, never on the EXPLAIN itself."""

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = '''
'Return [ctx: Db]
    SelectProject [ctx: Db] [projections: *]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'Let [ctx: Db] [name: $x]
    SelectProject [ctx: Db] [projections: *]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'Expr [ctx: Db] [expr: IF true THEN (SelectProject) END]
''''

*/

CREATE t:1 SET a = 1;

EXPLAIN RETURN (SELECT * FROM t);

EXPLAIN { SELECT * FROM t };

EXPLAIN LET $x = (SELECT * FROM t);

EXPLAIN IF true { SELECT * FROM t };
