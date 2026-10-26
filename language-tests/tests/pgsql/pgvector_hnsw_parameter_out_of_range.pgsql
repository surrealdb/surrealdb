/**
[env]
namespace = true
database = true
imports = ["pgsql/_pgvector_native.surql"]

[test]
reason = "The m storage parameter of an hnsw index is between 2 and 100"

[test.results]
request-error = "PostgreSQL DDL lowering error: invalid parameter value: value 1 out of bounds for option \"m\"; valid values are between \"2\" and \"100\""
*/

CREATE INDEX ON pgv_native USING hnsw (embedding vector_l2_ops) WITH (m = 1);
