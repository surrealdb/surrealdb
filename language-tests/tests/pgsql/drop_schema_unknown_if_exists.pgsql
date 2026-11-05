/**
[env]
namespace = true
database = true
imports = ["pgsql/_drop_schema_tables.surql"]

[test]
reason = "IF EXISTS makes DROP SCHEMA of a schema other than public do nothing, and every table stays"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, name: 'Ada' }]"
*/

DROP SCHEMA IF EXISTS nosuch CASCADE;

SELECT id, name FROM dsc_author;
