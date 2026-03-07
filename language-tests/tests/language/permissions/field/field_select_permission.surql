/**
[env]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }
imports = ["language/permissions/field/field_permissions_setup.surql"]

[test]
reason = "Field-level SELECT permission: record user can see 'secret' on owned records but not others"

[[test.results]]
value = "[{ name: 'alpha', secret: 'hidden' }]"

[[test.results]]
value = "[{ name: 'beta', secret: NONE }]"

*/
-- User:1 owns item:1 -> can see secret field
SELECT name, secret FROM item:1;
-- User:1 does not own item:2 -> secret field is hidden
SELECT name, secret FROM item:2;
