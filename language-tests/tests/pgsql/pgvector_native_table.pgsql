/**
[env]
namespace = true
database = true
imports = ["pgsql/_pgvector_native.surql"]

[test]
reason = "A column of a table defined in SurrealQL as an array of floats of a given length is a vector: text is written to it as a vector, it is indexed and searched by distance"

[[test.results]]
value = "[{ embedding: [1f, 2f, 3f], id: 1, label: 'a' }, { embedding: [4f, 5f, 6f], id: 2, label: 'b' }, { embedding: [1f, 0f, 0f], id: 3, label: 'c' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1 }, { id: 3 }]"

[[test.results]]
value = "[{ distance: 0f, id: 1 }, { distance: 5.196152422706632f, id: 2 }, { distance: 3.605551275463989f, id: 3 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 2]
        Sort [ctx: Db] [sort_keys: embedding = NULL OR embedding = NONE ASC, [embedding, [1f, 2f, 3f]].?(IF [0] = NULL OR [0] = NONE OR [1] = NULL OR [1] = NONE THEN NULL ELSE vector::distance::euclidean([0], [1]) END) ASC, keep: 2]
            KnnTopK [ctx: Db] [field: embedding, k: 2, distance: Euclidean, dimension: 3]
                TableScan [ctx: Db] [table: pgv_native, direction: Forward]
''''
*/

INSERT INTO pgv_native (id, embedding, label) VALUES (1, '[1,2,3]', 'a'), (2, '[4,5,6]', 'b'), (3, '[1,0,0]', 'c');

CREATE INDEX pgv_native_l2 ON pgv_native USING hnsw (embedding vector_l2_ops);

SELECT id FROM pgv_native ORDER BY embedding <-> '[1,2,3]' LIMIT 2;

SELECT id, embedding <-> '[1,2,3]' AS distance FROM pgv_native ORDER BY id;

EXPLAIN SELECT id FROM pgv_native ORDER BY embedding <-> '[1,2,3]' LIMIT 2;
