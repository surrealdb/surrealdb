/**
[env]
imports = ["upgrade/define/3.01.00_index_vector_absent_import.surql"]

[test]
upgrade = true
introduced = "3.1.0"
version = ">=3.4.0-a"
reason = """
A vector index whose column has stored no NONE or NULL holds every record of its
table, and a KNN scan of it answers ORDER BY vector::distance::<metric>(...)
LIMIT k with no sort above it. That needs the index to have recorded every value
shape it stored, which only a build on this checkout does. An index an older
binary built recorded none, so it keeps the sort until it is rebuilt; an index
that holds no entry for some record keeps it, rebuilt or not. The rows are the
same either way and are asserted on every hop in
3.01.00_index_vector_absent.surql.

The plan cannot span the 3.x line, so it lives here, gated to the current
checkout."""

[[test.repair]]
since = ">=3.4.0-a"
script = "upgrade/define/3.01.00_index_vector_absent_rebuild.surql"
unrepaired = "degraded"
reason = """
A vector index built before indexes recorded the value shapes they store may
hold no entry for a record whose vector is NONE or NULL, so ORDER BY
vector::distance::<metric>(...) LIMIT k over it sorts every row rather than
reading the nearest entries of the index. `REBUILD INDEX` starts the index
recording its shapes, and an index that holds every record then answers the
order."""

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 2]
        Sort [ctx: Db] [sort_keys: vector::distance::euclidean(point, [0f, 0f]) ASC, keep: 2]
            KnnScan [ctx: Db] [index: vabs_full_h_ix, k: 2, ef: 150, dimension: 2]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 2]
        Sort [ctx: Db] [sort_keys: vector::distance::euclidean(point, [0f, 0f]) ASC, keep: 2]
            KnnScan [ctx: Db] [index: vabs_full_d_ix, k: 2, ef: 100, dimension: 2]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 2]
        Sort [ctx: Db] [sort_keys: vector::distance::euclidean(point, [0f, 0f]) ASC, keep: 2]
            TableScan [ctx: Db] [table: vabs_none_h, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 2]
        Sort [ctx: Db] [sort_keys: vector::distance::euclidean(point, [0f, 0f]) ASC, keep: 2]
            TableScan [ctx: Db] [table: vabs_none_d, direction: Forward]
''''

*/
-- Every record holds a vector: the index answers the order.
EXPLAIN SELECT id FROM vabs_full_h ORDER BY vector::distance::euclidean(point, [0f, 0f]) LIMIT 2;
EXPLAIN SELECT id FROM vabs_full_d ORDER BY vector::distance::euclidean(point, [0f, 0f]) LIMIT 2;
-- A record without a vector keeps the sort, rebuilt or not.
EXPLAIN SELECT id FROM vabs_none_h ORDER BY vector::distance::euclidean(point, [0f, 0f]) LIMIT 2;
EXPLAIN SELECT id FROM vabs_none_d ORDER BY vector::distance::euclidean(point, [0f, 0f]) LIMIT 2;
