/**
[env]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }
imports = ["language/permissions/dml/dml_permissions_setup.surql"]

[test]
reason = "Record user: UPDATE on table with PERMISSIONS FOR update FULL succeeds"

[[test.results]]
value = "[{ id: update_full:1, owner: user:1, val: 'changed' }]"

*/
UPDATE update_full:1 SET val = 'changed';
