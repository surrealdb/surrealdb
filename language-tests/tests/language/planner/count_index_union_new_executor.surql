/**
[env]

timeout = 30000
context-timeout = 30000

[test]
reason = "A count over an IN list or an OR of record-disjoint index walks sums their key counts, and a count over a paged select of literals caps that count, with no record read"

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
value = '''
'SelectProject [ctx: Db] [projections: *]
    IndexCountScan [ctx: Db] [source: t, condition: n INSIDE [1, 2, 3]]
        IndexScan [ctx: Db] [table: t, index: t_n, access: = 1, direction: Forward, keys_only: yes]
        IndexScan [ctx: Db] [table: t, index: t_n, access: = 2, direction: Forward, keys_only: yes]
        IndexScan [ctx: Db] [table: t, index: t_n, access: = 3, direction: Forward, keys_only: yes]
''''

[[test.results]]
value = "[{ count: 30 }]"

[[test.results]]
value = "[{ count: 30 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    IndexCountScan [ctx: Db] [source: t, condition: n = 1 OR n = 2]
        IndexScan [ctx: Db] [table: t, index: t_n, access: = 1, direction: Forward, keys_only: yes]
        IndexScan [ctx: Db] [table: t, index: t_n, access: = 2, direction: Forward, keys_only: yes]
''''

[[test.results]]
value = "[{ count: 20 }]"

[[test.results]]
value = "[{ count: 20 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Aggregate [ctx: Db] [mode: GROUP ALL]
        Filter [ctx: Db] [predicate: n INSIDE [1, 1]]
            UnionIndexScan [ctx: Db] [table: t, branches: 2]
                IndexScan [ctx: Db] [table: t, index: t_n, access: = 1, direction: Forward]
                IndexScan [ctx: Db] [table: t, index: t_n, access: = 1, direction: Forward]
''''

[[test.results]]
value = "[{ count: 10 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Aggregate [ctx: Db] [mode: GROUP ALL]
        Filter [ctx: Db] [predicate: n = 1 OR m = 1]
            BitmapScan [ctx: Db] [table: t]
                BitmapOr [ctx: Db]
                    BitmapIndexScan [ctx: Db] [index: t_n, access: = 1]
                    BitmapIndexScan [ctx: Db] [index: t_m, access: = 1]
''''

[[test.results]]
value = "[{ count: 23 }]"

[[test.results]]
value = "[{ count: 23 }]"

[[test.results]]
value = "[{ count: 50 }]"

[[test.results]]
value = "[{ count: 50 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    IndexCountScan [ctx: Db] [source: t, condition: n INSIDE [1, 2]]
        IndexScan [ctx: Db] [table: t, index: t_n, access: = 1, direction: Forward, keys_only: yes]
        IndexScan [ctx: Db] [table: t, index: t_n, access: = 2, direction: Forward, keys_only: yes]
''''

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 10 }]"

[[test.results]]
value = "[{ count: 7 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Aggregate [ctx: Db] [mode: GROUP ALL]
        Subquery [ctx: Rt]
            SelectProject [ctx: Db] [projections: n]
                Limit [ctx: Db] [limit: 5]
                    Filter [ctx: Db] [predicate: n INSIDE [1, 2]]
                        UnionIndexScan [ctx: Db] [table: t, branches: 2]
                            IndexScan [ctx: Db] [table: t, index: t_n, access: = 1, direction: Forward]
                            IndexScan [ctx: Db] [table: t, index: t_n, access: = 2, direction: Forward]
''''

[[test.results]]
value = "[{ count: 5 }]"

[[test.results]]
value = "[{ count: 5 }]"
*/

DEFINE FIELD n ON t TYPE int;
DEFINE FIELD m ON t TYPE int;
FOR $i IN 0..100 {
	CREATE t CONTENT { n: $i % 10, m: $i % 7 };
};
DEFINE INDEX t_n ON TABLE t FIELDS n;
DEFINE INDEX t_m ON TABLE t FIELDS m;

-- An IN list is one walk per value.
EXPLAIN SELECT count() FROM t WHERE n IN [1, 2, 3] GROUP ALL;
SELECT count() FROM t WHERE n IN [1, 2, 3] GROUP ALL;
SELECT count() FROM t WITH NOINDEX WHERE n IN [1, 2, 3] GROUP ALL;

-- An OR of equalities on one index is one walk per branch.
EXPLAIN SELECT count() FROM t WHERE n = 1 OR n = 2 GROUP ALL;
SELECT count() FROM t WHERE n = 1 OR n = 2 GROUP ALL;
SELECT count() FROM t WITH NOINDEX WHERE n = 1 OR n = 2 GROUP ALL;

-- A value listed twice, or branches a record can satisfy together, are not summed.
EXPLAIN SELECT count() FROM t WHERE n IN [1, 1] GROUP ALL;
SELECT count() FROM t WHERE n IN [1, 1] GROUP ALL;
EXPLAIN SELECT count() FROM t WHERE n = 1 OR m = 1 GROUP ALL;
SELECT count() FROM t WHERE n = 1 OR m = 1 GROUP ALL;
SELECT count() FROM t WITH NOINDEX WHERE n = 1 OR m = 1 GROUP ALL;
SELECT count() FROM t WHERE n = 1 OR n > 5 GROUP ALL;
SELECT count() FROM t WITH NOINDEX WHERE n = 1 OR n > 5 GROUP ALL;

-- A count over a paged select of literals is the walk's count, capped by the paging.
EXPLAIN SELECT count() FROM (SELECT 1 FROM t WHERE n IN [1, 2] LIMIT 5) GROUP ALL;
SELECT count() FROM (SELECT 1 FROM t WHERE n IN [1, 2] LIMIT 5) GROUP ALL;
SELECT count() FROM (SELECT 1 FROM t WHERE n IN [1, 2] START 18 LIMIT 5) GROUP ALL;
SELECT count() FROM (SELECT 1 FROM t WHERE n IN [1, 2] START 30 LIMIT 5) GROUP ALL;
SELECT count() FROM (SELECT 1 FROM t WHERE n = 3 LIMIT 1000) GROUP ALL;
SELECT count() FROM (SELECT 1 FROM t LIMIT 7) GROUP ALL;

-- A select that projects a field, or sorts, keeps its ordinary plan and its answer.
EXPLAIN SELECT count() FROM (SELECT n FROM t WHERE n IN [1, 2] LIMIT 5) GROUP ALL;
SELECT count() FROM (SELECT n FROM t WHERE n IN [1, 2] LIMIT 5) GROUP ALL;
SELECT count() FROM (SELECT 1 FROM t WHERE n IN [1, 2] ORDER BY m LIMIT 5) GROUP ALL;
