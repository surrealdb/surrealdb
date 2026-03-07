/**
[env]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }
imports = ["language/permissions/dml/dml_permissions_setup.surql"]

[test]
reason = "Record user: SELECT on table with PERMISSIONS FOR select NONE returns empty"

[[test.results]]
value = "[]"

*/
SELECT * FROM select_none ORDER BY id;
