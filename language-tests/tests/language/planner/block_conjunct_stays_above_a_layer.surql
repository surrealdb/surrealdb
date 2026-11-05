/**
[env]

[test]
reason = "A conjunct written as a block stays above a computed-field layer: it is judged where it was written, so a row an earlier conjunct rejects never reaches the THROW inside it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 10, id: t:1, z: 2 }]"

[[test.results]]
value = "[{ a: 2, b: 20, id: t:2, z: 3 }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: IF a = 1 THEN (
  Sequence
    Throw['boom']
) ELSE (
  Sequence
    Eval[true]
) END AND b = 99]
        ProjectLayer [ctx: Db] [columns: z = a + 1 @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: IF a = 1 THEN (
  Sequence
    Throw['boom']
) ELSE (
  Sequence
    Eval[true]
) END]
        ProjectLayer [ctx: Db] [columns: z = a + 1 @computed]
            TableScan [ctx: Db] [table: t, direction: Forward, predicate: b = 99, pre_decode_filter: yes]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: z = 99 AND IF a = 1 THEN (
  Sequence
    Throw['boom']
) ELSE (
  Sequence
    Eval[true]
) END]
        ProjectLayer [ctx: Db] [columns: z = a + 1 @computed]
            TableScan [ctx: Db] [table: t, direction: Forward]
"'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "An error occurred: boom"

*/
DEFINE TABLE t SCHEMALESS;
DEFINE INDEX ia ON t FIELDS a;
DEFINE FIELD z ON t COMPUTED a + 1;
CREATE t:1 SET a = 1, b = 10;
CREATE t:2 SET a = 2, b = 20;

-- A block conjunct stays above the layer, whatever precedes it.
EXPLAIN SELECT * FROM t WHERE (IF a = 1 { THROW 'boom' } ELSE { true }) AND b = 99;
EXPLAIN SELECT * FROM t WHERE b = 99 AND (IF a = 1 { THROW 'boom' } ELSE { true });
EXPLAIN SELECT * FROM t WHERE z = 99 AND (IF a = 1 { THROW 'boom' } ELSE { true });

-- A row an earlier conjunct rejects does not raise.
SELECT * FROM t WHERE z = 99 AND (IF a = 1 { THROW 'boom' } ELSE { true });
SELECT * FROM t WHERE z = 99 AND false AND (IF a = 1 { THROW 'boom' } ELSE { true });
SELECT * FROM t WHERE b = 99 AND (IF a = 1 { THROW 'boom' } ELSE { true });
SELECT * FROM t WHERE (IF a = 1 { THROW 'boom' } ELSE { true }) AND b = 99;
