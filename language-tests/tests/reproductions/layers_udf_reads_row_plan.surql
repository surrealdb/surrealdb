/**
[env]

[test]
reason = """
	Plan companion to layers_udf_reads_row: a user-defined function
	call is evaluated against the row and its reads cannot be seen from the
	call, so the layer deriving the computed field stands beneath the
	projection or the filter that carries the call, and the body is not
	unfolded into the plan.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

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
    ProjectLayer [ctx: Db] [columns: c = a + 1 : int @computed]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: x]
    ProjectLayer [ctx: Db] [columns: c = a + 1 : int @computed]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: fn::f() > 1]
        ProjectLayer [ctx: Db] [columns: c = a + 1 : int @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: x, c]
    ProjectLayer [ctx: Db] [columns: c = a + 1 : int @computed]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''
*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD a ON t TYPE int;
DEFINE FIELD c ON t TYPE int COMPUTED a + 1;
DEFINE FUNCTION fn::f() { RETURN c; };
DEFINE FUNCTION fn::row() { RETURN $this; };
CREATE t:1 SET a = 1;

EXPLAIN SELECT fn::f() AS x FROM t;
EXPLAIN SELECT fn::row() AS x FROM t;
EXPLAIN SELECT id FROM t WHERE fn::f() > 1;
EXPLAIN SELECT fn::f() AS x, c FROM t;
