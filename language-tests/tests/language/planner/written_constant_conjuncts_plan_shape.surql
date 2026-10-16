/**
[env]

[test]
reason = "A written neutral literal (AND true, OR false) is dropped from a predicate and a decided one (AND false) replaces it, and a conjunct that reads no row is judged once ahead of the scan, so the operator tree and its attributes are the same whether the constant was written or folded"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: b = 'x', pre_decode_filter: yes]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: b = 'x', pre_decode_filter: yes]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: b = 'x', pre_decode_filter: yes]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: b = 'x', pre_decode_filter: yes]
"'''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [table: t, index: ia, access: = 1, direction: Forward]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: b = 'x' AND c = 'y', pre_decode_filter: yes]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: b = 'x' AND c = 'y', pre_decode_filter: yes]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: b = 'x' AND c = 'y', pre_decode_filter: yes]
"'''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: false]
        Union [ctx: Db]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: false]
        Union [ctx: Db]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: false]
        Union [ctx: Db]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: false]
        Union [ctx: Db]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [once: 1 = 2]
        Union [ctx: Db]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [once: 3 = 4, predicate: b = 'x']
        TableScan [ctx: Db] [table: t, direction: Forward]
"'''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [once: 3 = 4]
        Union [ctx: Db]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [once: 3 = 4, predicate: b = 'x' AND c = 'y']
        TableScan [ctx: Db] [table: t, direction: Forward]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [once: NONE = 1, predicate: b = 'x']
        TableScan [ctx: Db] [table: t, direction: Forward]
"'''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [once: NONE = 1]
        Union [ctx: Db]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [once: NONE = 1, predicate: b = 'x' AND c = 'y']
        TableScan [ctx: Db] [table: t, direction: Forward]
"'''

*/

DEFINE TABLE t SCHEMALESS;
DEFINE INDEX ia ON t FIELDS a;

-- A neutral literal vanishes wherever it is written, and the predicate goes into the scan.
EXPLAIN SELECT * FROM t WHERE b = 'x' AND true;
EXPLAIN SELECT * FROM t WHERE true AND b = 'x';
EXPLAIN SELECT * FROM t WHERE b = 'x' OR false;
EXPLAIN SELECT * FROM t WHERE false OR b = 'x';
EXPLAIN SELECT * FROM t WHERE a = 1 AND true;
EXPLAIN SELECT * FROM t WHERE b = 'x' AND true AND c = 'y';
EXPLAIN SELECT * FROM t WHERE true AND b = 'x' AND true AND c = 'y';
EXPLAIN SELECT * FROM t WHERE (b = 'x' OR false) AND (c = 'y' OR false);

-- A literal that decides the predicate is its predicate, wherever it is written.
EXPLAIN SELECT * FROM t WHERE false;
EXPLAIN SELECT * FROM t WHERE a = 1 AND false;
EXPLAIN SELECT * FROM t WHERE false AND a = 1;
EXPLAIN SELECT * FROM t WHERE b = 'x' AND false AND c = 'y';

-- A conjunct that reads no row is judged once, whether it is a constant or a parameter.
EXPLAIN SELECT * FROM t WHERE 1 = 2;
EXPLAIN SELECT * FROM t WHERE b = 'x' AND (3 = 4);
EXPLAIN SELECT * FROM t WHERE (3 = 4) AND b = 'x';
EXPLAIN SELECT * FROM t WHERE b = 'x' AND (3 = 4) AND c = 'y';
EXPLAIN SELECT * FROM t WHERE b = 'x' AND $p = 1;
EXPLAIN SELECT * FROM t WHERE $p = 1 AND b = 'x';
EXPLAIN SELECT * FROM t WHERE b = 'x' AND $p = 1 AND c = 'y';
