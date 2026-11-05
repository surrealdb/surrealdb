/**
[env]
namespace = true
database = true
imports = ["pgsql/_drop_schema_tables.surql"]

[test]
reason = "The system schemas cannot be dropped, with CASCADE or without"

[test.results]
request-error = "PostgreSQL DDL lowering error: unsupported expression: DROP SCHEMA pg_catalog: the schema is required by the database system"
*/

DROP SCHEMA pg_catalog CASCADE;
