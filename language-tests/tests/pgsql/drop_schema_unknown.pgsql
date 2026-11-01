/**
[env]
namespace = true
database = true
imports = ["pgsql/_drop_schema_tables.surql"]

[test]
reason = "A schema other than public does not exist: DROP SCHEMA names it as the undefined thing, PostgreSQL's 3F000 answered as a missing relation"

[test.results]
request-error = "PostgreSQL DDL lowering error: schema or table \"nosuch\" does not exist"
*/

DROP SCHEMA nosuch;
