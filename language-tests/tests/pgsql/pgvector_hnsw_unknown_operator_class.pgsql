/**
[env]
namespace = true
database = true
imports = ["pgsql/_pgvector_native.surql"]

[test]
reason = "An hnsw index over an operator class that does not exist is refused"

[test.results]
request-error = "PostgreSQL DDL lowering error: operator class \"int4_ops\" does not exist for access method \"hnsw\""
*/

CREATE INDEX ON pgv_native USING hnsw (embedding int4_ops);
