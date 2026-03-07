/**
[env]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }
imports = ["language/permissions/dml/dml_permissions_setup.surql"]

[test]
reason = "Record user: INSERT respects table CREATE permissions (NONE/FULL/WHERE)"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: create_full:ins, owner: user:1, val: 'yes' }]"

[[test.results]]
value = "[{ id: create_where:ins_ok, owner: user:1, val: 'mine' }]"

[[test.results]]
value = "[]"

*/
-- NONE: denied
INSERT INTO create_none { id: create_none:ins, val: 'no', owner: user:1 };
-- FULL: allowed
INSERT INTO create_full { id: create_full:ins, val: 'yes', owner: user:1 };
-- WHERE owner = $auth: allowed when owner matches
INSERT INTO create_where { id: create_where:ins_ok, val: 'mine', owner: user:1 };
-- WHERE owner = $auth: denied when owner doesn't match
INSERT INTO create_where { id: create_where:ins_bad, val: 'theirs', owner: user:2 };
