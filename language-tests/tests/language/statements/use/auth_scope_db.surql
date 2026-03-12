/**
[env]
namespace = "ns1"
database = "db1"
auth = { namespace = "ns1", database = "db1", level = "editor" }
imports = ["language/statements/use/auth_scope_db_setup.surql"]

[test]
reason = "Database-level users cannot USE a database outside their auth scope"

[[test.results]]
value = "[{ id: data:1, value: 'db1 secret' }]"

[[test.results]]
error = "You don't have permission to change to the db2 database"

[[test.results]]
value = "[{ id: data:1, value: 'db1 secret' }]"

*/

-- DB1 editor can read their own data
SELECT * FROM data;

-- DB1 editor cannot switch to db2
USE DB db2;

-- Confirm we're still in db1 after the failed USE
SELECT * FROM data;
