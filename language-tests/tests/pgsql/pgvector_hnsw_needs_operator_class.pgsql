/**
[env]
namespace = true
database = true
imports = ["pgsql/_pgvector_native.surql"]

[test]
reason = "An hnsw index names an operator class, which pgvector has no default for"

[test.results]
request-error = "PostgreSQL DDL lowering error: data type vector has no default operator class for access method \"hnsw\""
*/

CREATE INDEX ON pgv_native USING hnsw (embedding);
