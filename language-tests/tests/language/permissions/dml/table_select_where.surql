/**
[env]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }
imports = ["language/permissions/dml/dml_permissions_setup.surql"]

[test]
reason = "Record user: SELECT on table with PERMISSIONS FOR select WHERE owner = $auth returns only owned records"

[[test.results]]
value = "[{ id: select_where:1, owner: user:1, val: 'a' }]"

*/
SELECT * FROM select_where ORDER BY id;
