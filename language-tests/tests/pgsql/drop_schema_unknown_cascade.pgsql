/**
[env]
namespace = true
database = true
imports = ["pgsql/_drop_schema_tables.surql"]

[test]
reason = "CASCADE does not make a schema other than public exist"

[test.results]
request-error = "PostgreSQL DDL lowering error: schema or table \"nosuch\" does not exist"
*/

DROP SCHEMA nosuch CASCADE;
