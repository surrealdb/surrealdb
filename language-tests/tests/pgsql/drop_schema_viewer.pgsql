/**
[env]
namespace = true
database = true
auth = { level = "viewer" }
imports = ["pgsql/_drop_schema_tables.surql"]

[test]
reason = "A viewer is refused DROP SCHEMA public CASCADE as it is refused removing each table, and no table is removed"

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

[[test.results]]
value = "[{ id: 1, name: 'Ada' }]"
*/

DROP SCHEMA public CASCADE;

SELECT id, name FROM dsc_author;
