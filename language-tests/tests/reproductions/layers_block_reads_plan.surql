/**
[env]

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
'SelectProject [ctx: Db] [projections: x]
    ProjectLayer [ctx: Db] [columns: c = a + 1 : int @computed]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: (
  Sequence
    Eval[c]
) > 1]
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
