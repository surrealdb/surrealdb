/**
[env]
namespace = true
database = true
imports = ["pgsql/_pgvector_indexed.surql"]

[test]
reason = "A distance ordering with a literal limit is answered from the hnsw index over the column when it is the one index and ranks by the same distance, and by a scan that keeps the nearest rows otherwise; an ordering that is not a nearest-neighbour search sorts"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 5]
        Sort [ctx: Db] [sort_keys: embedding = NULL OR embedding = NONE ASC, [embedding, [1f, 2f, 3f]].?(IF [0] = NULL OR [0] = NONE OR [1] = NULL OR [1] = NONE THEN NULL ELSE vector::distance::euclidean([0], [1]) END) ASC, keep: 5]
            KnnScan [ctx: Db] [index: pgv_indexed_embedding, k: 5, ef: 40, dimension: 3]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 5]
        Sort [ctx: Db] [sort_keys: embedding = NULL OR embedding = NONE ASC, [embedding, [1f, 2f, 3f]].?(IF [0] = NULL OR [0] = NONE OR [1] = NULL OR [1] = NONE THEN NULL ELSE 1f - vector::similarity::cosine([0], [1]) END) ASC, keep: 5]
            KnnTopK [ctx: Db] [field: embedding, k: 5, distance: Cosine, dimension: 3]
                TableScan [ctx: Db] [table: pgv_indexed, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 5]
        Sort [ctx: Db] [sort_keys: direction = NULL OR direction = NONE ASC, [direction, [1f, 2f, 3f]].?(IF [0] = NULL OR [0] = NONE OR [1] = NULL OR [1] = NONE THEN NULL ELSE 1f - vector::similarity::cosine([0], [1]) END) ASC, keep: 5]
            KnnTopK [ctx: Db] [field: direction, k: 5, distance: Cosine, dimension: 3]
                TableScan [ctx: Db] [table: pgv_indexed, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 5, offset: 10]
        Sort [ctx: Db] [sort_keys: embedding = NULL OR embedding = NONE ASC, [embedding, [1f, 2f, 3f]].?(IF [0] = NULL OR [0] = NONE OR [1] = NULL OR [1] = NONE THEN NULL ELSE vector::distance::euclidean([0], [1]) END) ASC, keep: 15]
            KnnScan [ctx: Db] [index: pgv_indexed_embedding, k: 15, ef: 40, dimension: 3]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Sort [ctx: Db] [sort_keys: embedding = NULL OR embedding = NONE ASC, [embedding, [1f, 2f, 3f]].?(IF [0] = NULL OR [0] = NONE OR [1] = NULL OR [1] = NONE THEN NULL ELSE vector::distance::euclidean([0], [1]) END) ASC]
        TableScan [ctx: Db] [table: pgv_indexed, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 5]
        Sort [ctx: Db] [sort_keys: embedding = NULL OR embedding = NONE DESC, [embedding, [1f, 2f, 3f]].?(IF [0] = NULL OR [0] = NONE OR [1] = NULL OR [1] = NONE THEN NULL ELSE vector::distance::euclidean([0], [1]) END) DESC, keep: 5]
            TableScan [ctx: Db] [table: pgv_indexed, direction: Forward]
''''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 5]
        Sort [ctx: Db] [sort_keys: embedding = NULL OR embedding = NONE ASC, [embedding, [1f, 2f, 3f]].?(IF [0] = NULL OR [0] = NONE OR [1] = NULL OR [1] = NONE THEN NULL ELSE vector::distance::euclidean([0], [1]) END) ASC, keep: 5]
            Filter [ctx: Db] [predicate: IF label = NULL OR label = NONE OR 'a' = NULL OR 'a' = NONE THEN NULL ELSE label != 'a' END]
                KnnScan [ctx: Db] [index: pgv_indexed_embedding, k: 5, ef: 40, dimension: 3, predicate: IF label = NULL OR label = NONE OR 'a' = NULL OR 'a' = NONE THEN NULL ELSE label != 'a' END]
"'''
*/

EXPLAIN SELECT id FROM pgv_indexed ORDER BY embedding <-> '[1,2,3]' LIMIT 5;

EXPLAIN SELECT id FROM pgv_indexed ORDER BY embedding <=> '[1,2,3]' LIMIT 5;

EXPLAIN SELECT id FROM pgv_indexed ORDER BY direction <=> '[1,2,3]' LIMIT 5;

EXPLAIN SELECT id FROM pgv_indexed ORDER BY embedding <-> '[1,2,3]' LIMIT 5 OFFSET 10;

EXPLAIN SELECT id FROM pgv_indexed ORDER BY embedding <-> '[1,2,3]';

EXPLAIN SELECT id FROM pgv_indexed ORDER BY embedding <-> '[1,2,3]' DESC LIMIT 5;

EXPLAIN SELECT id FROM pgv_indexed WHERE label <> 'a' ORDER BY embedding <-> '[1,2,3]' LIMIT 5;
