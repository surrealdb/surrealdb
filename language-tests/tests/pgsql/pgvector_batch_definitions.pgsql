/**
[env]
namespace = true
database = true

[test]
reason = "A table, its hnsw index and the rows written to it are defined in one request, so the columns they name are those an earlier statement of the request defined"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ embedding: [1f, 0f], id: 1 }, { embedding: [0f, 1f], id: 2 }]"

[[test.results]]
value = "[{ id: 1 }]"
*/

CREATE EXTENSION IF NOT EXISTS vector; CREATE TABLE pgv_batch (id INT PRIMARY KEY, embedding vector(2)); CREATE INDEX ON pgv_batch USING hnsw (embedding vector_cosine_ops); INSERT INTO pgv_batch (id, embedding) VALUES (1, '[1,0]'), (2, '[0,1]'); SELECT id FROM pgv_batch ORDER BY embedding <=> '[1,0.1]' LIMIT 1;
