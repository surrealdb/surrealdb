/**
[env]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }
imports = ["language/permissions/dml/dml_permissions_setup.surql"]

[test]
reason = "Record user: UPDATE with PERMISSIONS FOR update WHERE owner = $auth - only owned records updated"

[[test.results]]
value = "[{ id: update_where:1, owner: user:1, val: 'changed' }]"

[[test.results]]
value = "[]"

*/
UPDATE update_where:1 SET val = 'changed';
UPDATE update_where:2 SET val = 'nope';
