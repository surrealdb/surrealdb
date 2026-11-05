/**
[env]
namespace = true
database = true

[test]
reason = "pgvector: ordering by a distance operator or function ranks rows by Euclidean distance, cosine distance, negated inner product and Manhattan distance, with and without an hnsw index of the same distance"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ embedding: [1f, 2f, 3f], id: 1, label: 'a' }, { embedding: [4f, 5f, 6f], id: 2, label: 'b' }, { embedding: [1f, 0f, 0f], id: 3, label: 'c' }, { embedding: [0f, 1f, 0f], id: 4, label: 'd' }, { embedding: NULL, id: 5, label: 'e' }]"

[[test.results]]
value = "[{ id: 1 }, { id: 4 }, { id: 3 }]"

[[test.results]]
value = "[{ id: 3 }, { id: 2 }, { id: 1 }]"

[[test.results]]
value = "[{ id: 2 }, { id: 1 }, { id: 4 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 3 }, { id: 4 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1 }, { id: 4 }, { id: 3 }]"

[[test.results]]
value = "[{ id: 3 }, { id: 2 }, { id: 1 }]"

[[test.results]]
value = "[{ id: 2 }, { id: 1 }, { id: 4 }]"

[[test.results]]
value = "[{ id: 4 }, { id: 3 }, { id: 2 }]"

[[test.results]]
value = "[{ id: 4 }, { id: 3 }]"

[[test.results]]
value = "[{ id: 2 }, { id: 3 }]"
*/

CREATE TABLE pgv_order (id INT PRIMARY KEY, embedding vector(3), label TEXT);

INSERT INTO pgv_order (id, embedding, label) VALUES (1, '[1,2,3]', 'a'), (2, '[4,5,6]', 'b'), (3, '[1,0,0]', 'c'), (4, '[0,1,0]', 'd'), (5, NULL, 'e');

SELECT id FROM pgv_order ORDER BY embedding <-> '[1,2,3]' LIMIT 3;

SELECT id FROM pgv_order ORDER BY embedding <=> '[1,0,0]' LIMIT 3;

SELECT id FROM pgv_order ORDER BY embedding <#> '[1,2,3]' LIMIT 3;

SELECT id FROM pgv_order ORDER BY embedding <+> '[1,2,3]', id LIMIT 3;

CREATE INDEX pgv_order_l2 ON pgv_order USING hnsw (embedding vector_l2_ops);

CREATE INDEX pgv_order_cosine ON pgv_order USING hnsw (embedding vector_cosine_ops);

SELECT id FROM pgv_order ORDER BY embedding <-> '[1,2,3]' LIMIT 3;

SELECT id FROM pgv_order ORDER BY embedding <=> '[1,0,0]' LIMIT 3;

SELECT id FROM pgv_order ORDER BY embedding <#> '[1,2,3]' LIMIT 3;

SELECT id FROM pgv_order ORDER BY l2_distance(embedding, '[1,2,3]') LIMIT 3 OFFSET 1;

SELECT id FROM pgv_order WHERE label <> 'a' ORDER BY embedding <-> '[1,2,3]' LIMIT 2;

SELECT id FROM pgv_order WHERE embedding IS NOT NULL ORDER BY embedding <-> '[1,2,3]' DESC LIMIT 2;
