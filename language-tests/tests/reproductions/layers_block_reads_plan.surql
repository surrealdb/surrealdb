/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = """
	Plan companion to layers_block_reads: a projection whose block, IF
	branch or nested SELECT over `$parent` reads the computed field `c` plans
	with the layer deriving `c` above the scan and beneath the projection; a
	WHERE whose block reads `c` keeps the filter above that layer.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, c: 2, id: t:1 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: x]
    Compute [ctx: Db] [fields: x = (Return)]
      x.subquery: Return [ctx: Db]
          Expr [ctx: Db] [expr: c]
        ProjectLayer [ctx: Db] [columns: c = a + 1 : int @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: x]
    Compute [ctx: Db] [fields: x = IF a > 0 THEN (Expr) ELSE (Expr) END]
        ProjectLayer [ctx: Db] [columns: c = a + 1 : int @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: x]
    Compute [ctx: Db] [fields: x = (UnwrapExactlyOne)]
      x.subquery: UnwrapExactlyOne [ctx: Db]
          ProjectValue [ctx: Db] [expr: c]
              DynamicScan [ctx: Db] [source: $parent]
        ProjectLayer [ctx: Db] [columns: c = a + 1 : int @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: (Expr) > 1]
      predicate.subquery: Expr [ctx: Db] [expr: c]
        ProjectLayer [ctx: Db] [columns: c = a + 1 : int @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''
*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD a ON t TYPE int;
DEFINE FIELD c ON t TYPE int COMPUTED a + 1;
CREATE t:1 SET a = 1;

EXPLAIN SELECT { RETURN c } AS x FROM t;
EXPLAIN SELECT (IF a > 0 { c } ELSE { 0 }) AS x FROM t;
EXPLAIN SELECT (SELECT VALUE c FROM ONLY $parent) AS x FROM t;
EXPLAIN SELECT id FROM t WHERE { c } > 1;
