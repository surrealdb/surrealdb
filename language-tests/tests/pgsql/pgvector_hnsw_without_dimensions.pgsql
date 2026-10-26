/**
[env]
namespace = true
database = true
imports = ["pgsql/_pgvector_native.surql"]

[test]
reason = "An hnsw index covers a column of vectors of a given length"

[test.results]
request-error = "PostgreSQL DDL lowering error: invalid parameter value: column does not have dimensions"
*/

CREATE INDEX ON pgv_loose USING hnsw (embedding vector_l2_ops);
