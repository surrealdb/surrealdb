/**
[env]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }
imports = ["language/permissions/dml/dml_permissions_setup.surql"]

[test]
reason = "Record user: CREATE with PERMISSIONS FOR create WHERE owner = $auth - allowed when owner matches, denied otherwise"

[[test.results]]
value = "[{ id: create_where:ok, owner: user:1, val: 'mine' }]"

[[test.results]]
value = "[]"

*/
CREATE create_where:ok SET val = 'mine', owner = user:1;
CREATE create_where:bad SET val = 'theirs', owner = user:2;
