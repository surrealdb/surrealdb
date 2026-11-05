/**
[env]

[test]
reason = """
A mutation's WHERE is a predicate like a read's: binding substitutes the value
of each statement-scoped parameter it reads -- a LET binding, a client bind
parameter, a DEFINE PARAM readable with FULL -- so a bound written as a
parameter reads the index a literal bound reads. DELETE, UPDATE and UPSERT are
each pinned with EXPLAIN, beside the SELECT of the same predicate, for a LET
bound, a LET computed from an expression, and a DEFINE PARAM bound.

The write still judges its WHERE per record, so each write is paired with the
same write under WITH NOINDEX: the records the indexed write reaches must be
the records the forced scan reaches.

A name a block binds with its own LET is not substituted when the statement is
bound -- it has no value until the block runs -- and the write inside the block
still reaches the records the bound admits.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: plain:1, time: d'2025-01-01T00:00:00Z' }]"

[[test.results]]
value = "[{ id: plain:2, time: d'2025-03-01T00:00:00Z' }]"

[[test.results]]
value = "[{ id: plain:3, time: d'2025-09-01T00:00:00Z' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [table: plain, index: by_time, access: <d'2025-06-01T00:00:00Z', direction: Forward]
"'''

[[test.results]]
value = '''
"WriteOperator [ctx: Db] [form: DELETE]
    SelectProject [ctx: Db] [projections: *]
        DistinctRecords [ctx: Db]
            IndexScan [ctx: Db] [table: plain, index: by_time, access: <d'2025-06-01T00:00:00Z', direction: Forward]
"'''

[[test.results]]
value = '''
"WriteOperator [ctx: Db] [form: DELETE]
    SelectProject [ctx: Db] [projections: *]
        DistinctRecords [ctx: Db]
            IndexScan [ctx: Db] [table: plain, index: by_time, access: <d'2025-06-01T00:00:00Z', direction: Forward]
"'''

[[test.results]]
value = '''
"WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        DistinctRecords [ctx: Db]
            IndexScan [ctx: Db] [table: plain, index: by_time, access: <d'2025-06-01T00:00:00Z', direction: Forward]
"'''

[[test.results]]
value = '''
"WriteOperator [ctx: Db] [form: UPSERT]
    SelectProject [ctx: Db] [projections: *]
        DistinctRecords [ctx: Db]
            IndexScan [ctx: Db] [table: plain, index: by_time, access: <d'2025-06-01T00:00:00Z', direction: Forward]
"'''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: DELETE]
    SelectProject [ctx: Db] [projections: *]
        TableScan [ctx: Db] [table: plain, direction: Forward]
''''

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"WriteOperator [ctx: Db] [form: DELETE]
    SelectProject [ctx: Db] [projections: *]
        DistinctRecords [ctx: Db]
            IndexScan [ctx: Db] [table: plain, index: by_time, access: <d'2025-06-03T00:00:00Z', direction: Forward]
"'''

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"WriteOperator [ctx: Db] [form: DELETE]
    SelectProject [ctx: Db] [projections: *]
        DistinctRecords [ctx: Db]
            IndexScan [ctx: Db] [table: plain, index: by_time, access: <d'2025-06-01T00:00:00Z', direction: Forward]
"'''

[[test.results]]
value = "[plain:1, plain:2]"

[[test.results]]
value = "[plain:1, plain:2]"

[[test.results]]
value = "[{ hit: 1, id: plain:1, scan_hit: 1 }, { hit: 1, id: plain:2, scan_hit: 1 }, { hit: NONE, id: plain:3, scan_hit: NONE }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ hit: 1, id: copy:1, scan_hit: 1, time: d'2025-01-01T00:00:00Z' }, { hit: 1, id: copy:2, scan_hit: 1, time: d'2025-03-01T00:00:00Z' }, { id: copy:3, time: d'2025-09-01T00:00:00Z' }]"

[[test.results]]
value = "[{ hit: 1, id: plain:1, scan_hit: 1, time: d'2025-01-01T00:00:00Z' }, { hit: 1, id: plain:2, scan_hit: 1, time: d'2025-03-01T00:00:00Z' }]"

[[test.results]]
value = "[{ hit: 1, id: copy:1, scan_hit: 1, time: d'2025-01-01T00:00:00Z' }, { hit: 1, id: copy:2, scan_hit: 1, time: d'2025-03-01T00:00:00Z' }]"

[[test.results]]
value = "[[3], [3]]"

[[test.results]]
value = "[{ id: plain:4, time: d'2025-02-01T00:00:00Z' }]"

[[test.results]]
value = "[{ id: plain:4, time: d'2025-02-01T00:00:00Z' }]"

[[test.results]]
value = "[3]"

*/

DEFINE TABLE plain SCHEMALESS;
DEFINE FIELD time ON plain TYPE datetime;
DEFINE INDEX by_time ON plain FIELDS time;
CREATE plain:1 SET time = d'2025-01-01T00:00:00Z';
CREATE plain:2 SET time = d'2025-03-01T00:00:00Z';
CREATE plain:3 SET time = d'2025-09-01T00:00:00Z';

LET $cut = d'2025-06-01T00:00:00Z';

-- The plans: every form reads the index, as the literal bound does.
EXPLAIN SELECT * FROM plain WHERE time < $cut;
EXPLAIN DELETE plain WHERE time < d'2025-06-01T00:00:00Z';
EXPLAIN DELETE plain WHERE time < $cut;
EXPLAIN UPDATE plain SET x = 1 WHERE time < $cut;
EXPLAIN UPSERT plain SET x = 1 WHERE time < $cut;
EXPLAIN DELETE plain WITH NOINDEX WHERE time < $cut;

-- A LET computed from an expression is a value by the time the write is planned.
LET $since = d'2025-10-01T00:00:00Z' - 120d;
EXPLAIN DELETE plain WHERE time < $since;

-- A DEFINE PARAM readable with FULL is a value at plan time.
DEFINE PARAM $retention_cut VALUE d'2025-06-01T00:00:00Z';
EXPLAIN DELETE plain WHERE time < $retention_cut;

-- UPDATE through the index and through a forced scan reach the same records.
UPDATE plain SET hit += 1 WHERE time < $cut RETURN VALUE id;
UPDATE plain WITH NOINDEX SET scan_hit += 1 WHERE time < $cut RETURN VALUE id;
SELECT id, hit, scan_hit FROM plain ORDER BY id;

-- DELETE through the index and through a forced scan remove the same records.
DEFINE TABLE copy SCHEMALESS;
DEFINE FIELD time ON copy TYPE datetime;
DEFINE INDEX by_time ON copy FIELDS time;
INSERT INTO copy (SELECT * FROM plain);
DELETE plain WHERE time < $cut RETURN BEFORE;
DELETE copy WITH NOINDEX WHERE time < $cut RETURN BEFORE;
[(SELECT VALUE record::id(id) FROM plain ORDER BY id), (SELECT VALUE record::id(id) FROM copy ORDER BY id)];

-- A name a block binds has its value only once the block runs; the write
-- reaches the records the bound admits.
CREATE plain:4 SET time = d'2025-02-01T00:00:00Z';
{ LET $local = d'2025-06-01T00:00:00Z'; DELETE plain WHERE time < $local RETURN BEFORE };
SELECT VALUE record::id(id) FROM plain ORDER BY id;
