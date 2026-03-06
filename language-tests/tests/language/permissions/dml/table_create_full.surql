/**
[env]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }
imports = ["language/permissions/dml/dml_permissions_setup.surql"]

[test]
reason = "Record user: CREATE on table with PERMISSIONS FOR create FULL succeeds"

[[test.results]]
value = "[{ id: create_full:new, owner: user:1, val: 'allowed' }]"

*/
CREATE create_full:new SET val = 'allowed', owner = user:1;
