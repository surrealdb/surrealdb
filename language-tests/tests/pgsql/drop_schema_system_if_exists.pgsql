/**
[env]
namespace = true
database = true
imports = ["pgsql/_drop_schema_tables.surql"]

[test]
reason = "IF EXISTS does not make a system schema droppable: the schema exists"

[test.results]
request-error = "PostgreSQL DDL lowering error: unsupported expression: DROP SCHEMA information_schema: the schema is required by the database system"
*/

DROP SCHEMA IF EXISTS information_schema;
