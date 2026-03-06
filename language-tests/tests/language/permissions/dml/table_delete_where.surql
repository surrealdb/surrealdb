/**
[env]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }
imports = ["language/permissions/dml/dml_permissions_setup.surql"]

[test]
reason = "Record user: DELETE with PERMISSIONS FOR delete WHERE owner = $auth - only owned records deleted"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: delete_where:2, owner: user:2 }]"

*/
DELETE delete_where:1;
DELETE delete_where:2;
-- Only delete_where:2 should remain (owned by user:2, not deletable by user:1)
SELECT * FROM delete_where ORDER BY id;
