/**
[env]
namespace = true
database = true

[test]
reason = "pgvector: CREATE EXTENSION vector is accepted and defines nothing, a vector(n) column is created, and hnsw indexes are created for each operator class, with storage parameters and without"

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
value = "[{ embedding: [1f, 2f, 3f], id: 1 }]"
*/

CREATE EXTENSION vector;

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE pgv_ddl (id INT PRIMARY KEY, embedding vector(3), label TEXT);

CREATE INDEX pgv_ddl_l2 ON pgv_ddl USING hnsw (embedding vector_l2_ops) WITH (m = 8, ef_construction = 32);

CREATE INDEX pgv_ddl_cosine ON pgv_ddl USING hnsw (embedding vector_cosine_ops);

CREATE INDEX pgv_ddl_ip ON pgv_ddl USING hnsw (embedding vector_ip_ops);

CREATE INDEX pgv_ddl_l1 ON pgv_ddl USING hnsw (embedding vector_l1_ops);

CREATE INDEX IF NOT EXISTS pgv_ddl_l2 ON pgv_ddl USING hnsw (embedding vector_l2_ops);

INSERT INTO pgv_ddl (id, embedding, label) VALUES (1, '[1,2,3]', 'a') RETURNING id, embedding;
