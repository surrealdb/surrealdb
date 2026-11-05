/**
[env]

[test]
reason = "A filter over a subquery is judged beneath the subquery's projection, so the access path of the subquery's source serves it, whether the projection returns its rows unchanged, names the fields it keeps or lands a field under another name; a projection that computes a field keeps the filter above"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Subquery [ctx: Rt]
        SelectProject [ctx: Db] [projections: *]
            Filter [ctx: Db] [predicate: a = 1 OR a = 2]
                UnionIndexScan [ctx: Db] [table: t, branches: 2]
                    IndexScan [ctx: Db] [table: t, index: ia, access: = 1, direction: Forward]
                    IndexScan [ctx: Db] [table: t, index: ia, access: = 2, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Subquery [ctx: Rt]
        SelectProject [ctx: Db] [projections: *]
            IndexScan [ctx: Db] [table: t, index: ia, access: >time::now() - 1d, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Subquery [ctx: Rt]
        SelectProject [ctx: Db] [projections: *]
            Filter [ctx: Db] [predicate: string::len(b) > 1]
                TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Subquery [ctx: Rt]
        SelectProject [ctx: Db] [projections: a, b]
            IndexScan [ctx: Db] [table: t, index: ia, access: = 1, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Subquery [ctx: Rt]
        SelectProject [ctx: Db] [projections: a, b]
            Filter [ctx: Db] [predicate: a = 1 AND b = 2]
                BitmapScan [ctx: Db] [table: t]
                    BitmapAnd [ctx: Db]
                        BitmapIndexScan [ctx: Db] [index: ia, access: = 1]
                        BitmapIndexScan [ctx: Db] [index: ib, access: = 2]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Subquery [ctx: Rt]
        SelectProject [ctx: Db] [projections: a, b]
            Filter [ctx: Db] [predicate: c = 3]
                IndexScan [ctx: Db] [table: t, index: ia, access: = 1, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Subquery [ctx: Rt]
        SelectProject [ctx: Db] [projections: z]
            IndexScan [ctx: Db] [table: t, index: ia, access: = 1, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Subquery [ctx: Rt]
        SelectProject [ctx: Db] [projections: *, z]
            Filter [ctx: Db] [predicate: c = 2]
                IndexScan [ctx: Db] [table: t, index: ia, access: = 1, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Subquery [ctx: Rt]
        SelectProject [ctx: Db] [projections: *]
            Subquery [ctx: Rt]
                SelectProject [ctx: Db] [projections: z]
                    IndexScan [ctx: Db] [table: t, index: ia, access: = 1, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: z = 1]
        Subquery [ctx: Rt]
            SelectProject [ctx: Db] [projections: z]
                TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Filter [ctx: Db] [predicate: b = 1]
        Subquery [ctx: Rt]
            SelectProject [ctx: Db] [projections: a]
                TableScan [ctx: Db] [table: t, direction: Forward]
''''

*/

DEFINE TABLE t SCHEMALESS;
DEFINE INDEX ia ON t FIELDS a;
DEFINE INDEX ib ON t FIELDS b;

-- The projection returns its rows unchanged.
EXPLAIN SELECT * FROM (SELECT * FROM t) WHERE a = 1 OR a = 2;
EXPLAIN SELECT * FROM (SELECT * FROM t) WHERE a > time::now() - 1d;
EXPLAIN SELECT * FROM (SELECT * FROM t) WHERE string::len(b) > 1;

-- The projection names the fields it keeps, or lands one under another name.
EXPLAIN SELECT * FROM (SELECT a, b FROM t) WHERE a = 1;
EXPLAIN SELECT * FROM (SELECT a, b FROM t) WHERE a = 1 AND b = 2;
EXPLAIN SELECT * FROM (SELECT a, b FROM t WHERE c = 3) WHERE a = 1;
EXPLAIN SELECT * FROM (SELECT a AS z FROM t) WHERE z = 1;
EXPLAIN SELECT * FROM (SELECT *, a AS z FROM t) WHERE z = 1 AND c = 2;
EXPLAIN SELECT * FROM (SELECT * FROM (SELECT a AS z FROM t) WHERE z = 1) WHERE z = 1;

-- The projection computes the field, or the filter reads a field it does not keep.
EXPLAIN SELECT * FROM (SELECT a + 1 AS z FROM t) WHERE z = 1;
EXPLAIN SELECT * FROM (SELECT a FROM t) WHERE b = 1;
