/**
[env]
namespace = true
database = true
imports = ["pgsql/_drop_schema_tables.surql"]

[test]
reason = "IF EXISTS does not lift the refusal of DROP SCHEMA public without CASCADE: the schema exists"

[test.results]
request-error = "PostgreSQL DDL lowering error: unsupported expression: DROP SCHEMA public: the database holds tables that depend on it; use CASCADE"
*/

DROP SCHEMA IF EXISTS public RESTRICT;
