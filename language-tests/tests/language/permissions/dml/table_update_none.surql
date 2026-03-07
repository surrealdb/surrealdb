/**
[env]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }
imports = ["language/permissions/dml/dml_permissions_setup.surql"]

[test]
reason = "Record user: UPDATE on table with PERMISSIONS FOR update NONE is silently denied"

[[test.results]]
value = "[]"

*/
UPDATE update_none:1 SET val = 'nope';
