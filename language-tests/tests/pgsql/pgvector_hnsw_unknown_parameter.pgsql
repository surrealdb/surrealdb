/**
[env]
namespace = true
database = true
imports = ["pgsql/_pgvector_native.surql"]

[test]
reason = "An hnsw index has no storage parameter other than m and ef_construction"

[test.results]
request-error = "PostgreSQL DDL lowering error: invalid parameter value: unrecognized parameter \"fillfactor\""
*/

CREATE INDEX ON pgv_native USING hnsw (embedding vector_l2_ops) WITH (fillfactor = 70);
