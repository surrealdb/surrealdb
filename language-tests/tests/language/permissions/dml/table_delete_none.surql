/**
[env]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }
imports = ["language/permissions/dml/dml_permissions_setup.surql"]

[test]
reason = "Record user: DELETE on table with PERMISSIONS FOR delete NONE is silently denied"

[[test.results]]
value = "[]"

*/
DELETE delete_none:1;
