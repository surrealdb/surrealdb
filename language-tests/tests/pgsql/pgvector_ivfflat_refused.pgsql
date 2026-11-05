/**
[env]
namespace = true
database = true
imports = ["pgsql/_pgvector_native.surql"]

[test]
reason = "An ivfflat index is not supported"

[test.results]
request-error = "PostgreSQL DDL lowering error: unsupported statement: access method \"ivfflat\""
*/

CREATE INDEX ON pgv_native USING ivfflat (embedding vector_l2_ops);
