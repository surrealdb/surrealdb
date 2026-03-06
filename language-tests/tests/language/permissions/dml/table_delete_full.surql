/**
[env]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }
imports = ["language/permissions/dml/dml_permissions_setup.surql"]

[test]
reason = "Record user: DELETE on table with PERMISSIONS FOR delete FULL succeeds"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/
DELETE delete_full:1;
-- Confirm it was deleted
SELECT * FROM delete_full;
