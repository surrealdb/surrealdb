/**
[env]
namespace = true
database = true

[test]
issue = 1726
reason = """
A mutation's `WHERE` reads a b-tree index for a bound held in a parameter, as a
`SELECT` does and as the same bound written as a literal does: a statement-scoped
parameter (a `LET` binding, a client bind parameter) is substituted into the
condition before the access path is chosen. Each `DELETE` and `UPDATE` changes
the records the same statement under `WITH NOINDEX` changes, on twin tables.
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
value = "NONE"

[[test.results]]
value = "[{ id: plain:1, n: 1, time: d'2025-06-02T00:00:00Z' }]"

[[test.results]]
value = "[{ id: plain:2, n: 2, time: d'2025-06-03T00:00:00Z' }]"

[[test.results]]
value = "[{ id: plain:3, n: 3, time: d'2025-06-04T00:00:00Z' }]"

[[test.results]]
value = "[{ id: plain:4, n: 4, time: d'2025-06-05T00:00:00Z' }]"

[[test.results]]
value = "[{ id: plain:5, n: 5, time: d'2025-06-06T00:00:00Z' }]"

[[test.results]]
value = "[{ id: twin:1, n: 1, time: d'2025-06-02T00:00:00Z' }]"

[[test.results]]
value = "[{ id: twin:2, n: 2, time: d'2025-06-03T00:00:00Z' }]"

[[test.results]]
value = "[{ id: twin:3, n: 3, time: d'2025-06-04T00:00:00Z' }]"

[[test.results]]
value = "[{ id: twin:4, n: 4, time: d'2025-06-05T00:00:00Z' }]"

[[test.results]]
value = "[{ id: twin:5, n: 5, time: d'2025-06-06T00:00:00Z' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"WriteOperator [ctx: Db] [form: DELETE]
    SelectProject [ctx: Db] [projections: *]
        IndexScan [ctx: Db] [table: plain, index: by_time, access: <d'2025-06-04T00:00:00Z', direction: Forward]
"'''

[[test.results]]
value = '''
"WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        IndexScan [ctx: Db] [table: plain, index: by_time, access: <d'2025-06-04T00:00:00Z', direction: Forward]
"'''

[[test.results]]
value = '''
"WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        IndexScan [ctx: Db] [table: plain, index: by_time, access: >=d'2025-06-04T00:00:00Z' <d'2025-06-05T00:00:00Z', direction: Forward]
"'''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: DELETE]
    SelectProject [ctx: Db] [projections: *]
        TableScan [ctx: Db] [table: plain, direction: Forward]
''''

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[4, 5]"

[[test.results]]
value = "[4, 5]"

[[test.results]]
value = "[{ n: 3, x: 1 }]"

[[test.results]]
value = "[{ n: 3, x: 1 }]"

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
value = "NONE"

[[test.results]]
value = "[{ id: hall:1, n: 1, time: d'2025-06-02T00:00:00Z' }]"

[[test.results]]
value = "[{ id: hall:2, n: 2, time: d'2025-06-03T00:00:00Z' }]"

[[test.results]]
value = "[{ id: hall:3, n: 3, time: d'2025-06-05T00:00:00Z' }]"

[[test.results]]
value = "[{ id: hall_twin:1, n: 1, time: d'2025-06-02T00:00:00Z' }]"

[[test.results]]
value = "[{ id: hall_twin:2, n: 2, time: d'2025-06-03T00:00:00Z' }]"

[[test.results]]
value = "[{ id: hall_twin:3, n: 3, time: d'2025-06-05T00:00:00Z' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        IndexScan [ctx: Db] [table: hall, index: by_time, access: <d'2025-06-04T00:00:00Z', direction: Forward]
"'''

[[test.results]]
value = "[{ id: hall:1, n: 1, time: d'2025-06-03T00:00:00Z' }, { id: hall:2, n: 2, time: d'2025-06-04T00:00:00Z' }]"

[[test.results]]
value = "[{ id: hall_twin:1, n: 1, time: d'2025-06-03T00:00:00Z' }, { id: hall_twin:2, n: 2, time: d'2025-06-04T00:00:00Z' }]"

[[test.results]]
value = "[{ n: 1, time: d'2025-06-03T00:00:00Z' }, { n: 2, time: d'2025-06-04T00:00:00Z' }, { n: 3, time: d'2025-06-05T00:00:00Z' }]"

[[test.results]]
value = "[{ n: 1, time: d'2025-06-03T00:00:00Z' }, { n: 2, time: d'2025-06-04T00:00:00Z' }, { n: 3, time: d'2025-06-05T00:00:00Z' }]"

*/

DEFINE TABLE plain SCHEMALESS;
DEFINE FIELD time ON plain TYPE datetime;
DEFINE INDEX by_time ON plain FIELDS time;
DEFINE TABLE twin SCHEMALESS;
DEFINE FIELD time ON twin TYPE datetime;
DEFINE INDEX by_time ON twin FIELDS time;
CREATE plain:1 SET n = 1, time = d'2025-06-02T00:00:00Z';
CREATE plain:2 SET n = 2, time = d'2025-06-03T00:00:00Z';
CREATE plain:3 SET n = 3, time = d'2025-06-04T00:00:00Z';
CREATE plain:4 SET n = 4, time = d'2025-06-05T00:00:00Z';
CREATE plain:5 SET n = 5, time = d'2025-06-06T00:00:00Z';
CREATE twin:1 SET n = 1, time = d'2025-06-02T00:00:00Z';
CREATE twin:2 SET n = 2, time = d'2025-06-03T00:00:00Z';
CREATE twin:3 SET n = 3, time = d'2025-06-04T00:00:00Z';
CREATE twin:4 SET n = 4, time = d'2025-06-05T00:00:00Z';
CREATE twin:5 SET n = 5, time = d'2025-06-06T00:00:00Z';
LET $cut = d'2025-06-04T00:00:00Z';

EXPLAIN DELETE plain WHERE time < $cut;
EXPLAIN UPDATE plain SET x = 1 WHERE time < $cut;
EXPLAIN UPDATE plain SET x = 1 WHERE time >= $cut AND time < $cut + 1d;
EXPLAIN DELETE plain WITH NOINDEX WHERE time < $cut;

(DELETE plain WHERE time < $cut RETURN BEFORE).n;
(DELETE twin WITH NOINDEX WHERE time < $cut RETURN BEFORE).n;
(UPDATE plain SET x = 1 WHERE time >= $cut AND time < $cut + 1d RETURN AFTER).n;
(UPDATE twin WITH NOINDEX SET x = 1 WHERE time >= $cut AND time < $cut + 1d RETURN AFTER).n;
(DELETE plain WHERE time > $cut + 12h RETURN BEFORE).n;
(DELETE twin WITH NOINDEX WHERE time > $cut + 12h RETURN BEFORE).n;
SELECT n, x FROM plain ORDER BY n;
SELECT n, x FROM twin ORDER BY n;

-- An update that moves the indexed field it scans by: each record moves once.
DEFINE TABLE hall SCHEMALESS;
DEFINE FIELD time ON hall TYPE datetime;
DEFINE INDEX by_time ON hall FIELDS time;
DEFINE TABLE hall_twin SCHEMALESS;
DEFINE FIELD time ON hall_twin TYPE datetime;
DEFINE INDEX by_time ON hall_twin FIELDS time;
CREATE hall:1 SET n = 1, time = d'2025-06-02T00:00:00Z';
CREATE hall:2 SET n = 2, time = d'2025-06-03T00:00:00Z';
CREATE hall:3 SET n = 3, time = d'2025-06-05T00:00:00Z';
CREATE hall_twin:1 SET n = 1, time = d'2025-06-02T00:00:00Z';
CREATE hall_twin:2 SET n = 2, time = d'2025-06-03T00:00:00Z';
CREATE hall_twin:3 SET n = 3, time = d'2025-06-05T00:00:00Z';
LET $hcut = d'2025-06-04T00:00:00Z';
EXPLAIN UPDATE hall SET time = time + 1d WHERE time < $hcut;
UPDATE hall SET time = time + 1d WHERE time < $hcut;
UPDATE hall_twin WITH NOINDEX SET time = time + 1d WHERE time < $hcut;
SELECT n, time FROM hall ORDER BY n;
SELECT n, time FROM hall_twin ORDER BY n;
