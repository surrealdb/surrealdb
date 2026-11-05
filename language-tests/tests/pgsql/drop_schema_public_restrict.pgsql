/**
[env]
namespace = true
database = true
imports = ["pgsql/_drop_schema_tables.surql"]

[test]
reason = "DROP SCHEMA public without CASCADE is refused while the database holds a table, and the request does not run"

[test.results]
request-error = "PostgreSQL DDL lowering error: unsupported expression: DROP SCHEMA public: the database holds tables that depend on it; use CASCADE"
*/

DROP SCHEMA public;

SELECT id, title FROM dsc_post;
