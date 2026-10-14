/**
[env]
namespace = true
database = true

[test]
reason = """
The plans behind `knn_block_let_query_vector.surql`: a KNN whose query vector
is only known when the statement runs is served by a `KnnScan` on the ANN
index, with a `deferred` dimension because the scan evaluates the vector when
it runs, and not by a residual `Filter` over a table scan, where the KNN
operator matches no row. An indexed conjunct beside it becomes the scan's
prefilter. A brute-force KNN on a table with no index is served by a
`KnnTopK`, with a `deferred` dimension for the same reason. A vector the
planner can compute — a constant bound by a `LET` — is folded and carries its
dimension.
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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    KnnScan [ctx: Db] [index: hn, k: 1, ef: 40, dimension: deferred]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    KnnScan [ctx: Db] [index: dn, k: 1, ef: 40, dimension: deferred]
''''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: category = 'a']
        KnnScan [ctx: Db] [index: hn, k: 2, ef: 40, dimension: deferred, predicate: category = 'a']
            BitmapIndexScan [ctx: Db] [index: cat, access: = 'a']
"'''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    KnnTopK [ctx: Db] [field: point, k: 1, distance: Euclidean, dimension: deferred]
        TableScan [ctx: Db] [table: bpts, direction: Forward]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    KnnScan [ctx: Db] [index: hn, k: 1, ef: 40, dimension: deferred]
''''

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    KnnScan [ctx: Db] [index: hn, k: 1, ef: 40, dimension: 2]
''''
*/

DEFINE FIELD category ON pts TYPE string;
DEFINE INDEX hn ON pts FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
DEFINE INDEX cat ON pts FIELDS category;
DEFINE INDEX dn ON dpts FIELDS point DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32;
-- HNSW, a FOR variable
FOR $v IN [[5f, 0f]] { CREATE plan:hn SET p = (EXPLAIN SELECT VALUE id FROM pts WHERE point <|1,40|> $v); };
-- DiskANN, a FOR variable
FOR $v IN [[5f, 0f]] { CREATE plan:dn SET p = (EXPLAIN SELECT VALUE id FROM dpts WHERE point <|1,40|> $v); };
-- the indexed conjunct becomes the KNN's prefilter
FOR $v IN [[5f, 0f]] { CREATE plan:pre SET p = (EXPLAIN SELECT VALUE id FROM pts WHERE category = 'a' AND point <|2,40|> $v); };
-- brute force: no index on `bpts`
FOR $v IN [[5f, 0f]] { CREATE plan:bf SET p = (EXPLAIN SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> $v); };
-- a path rooted at a bound parameter
FOR $o IN [{ v: [5f, 0f] }] { CREATE plan:path SET p = (EXPLAIN SELECT VALUE id FROM pts WHERE point <|1,40|> $o.v); };
SELECT VALUE p FROM ONLY plan:hn;
SELECT VALUE p FROM ONLY plan:dn;
SELECT VALUE p FROM ONLY plan:pre;
SELECT VALUE p FROM ONLY plan:bf;
SELECT VALUE p FROM ONLY plan:path;
-- a constant bound by a LET of the block is folded
{ LET $q = [5f, 0f]; RETURN EXPLAIN SELECT VALUE id FROM pts WHERE point <|1,40|> $q; };
