/**
[env]
namespace = true
database = true
imports = ["pgsql/_pgvector_native.surql"]

[test]
reason = "ef_construction of an hnsw index is at least twice m"

[test.results]
request-error = "PostgreSQL DDL lowering error: invalid parameter value: ef_construction must be greater than or equal to 2 * m"
*/

CREATE INDEX ON pgv_native USING hnsw (embedding vector_l2_ops) WITH (m = 16, ef_construction = 31);
