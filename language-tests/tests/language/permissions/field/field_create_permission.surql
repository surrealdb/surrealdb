/**
[env]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }
imports = ["language/permissions/field/field_permissions_setup.surql"]

[test]
reason = "Field-level CREATE permission: 'locked' field with FOR create NONE is ignored when record user creates"

[[test.results]]
value = "[{ id: item:new, name: 'new', owner: user:1, readonly: 'initial', secret: 'my_secret' }]"

[[test.results]]
value = "[{ locked: NONE, name: 'new', readonly: 'initial' }]"

*/
CREATE item:new SET name = 'new', secret = 'my_secret', locked = 'should_be_ignored', readonly = 'initial', owner = user:1;
SELECT name, locked, readonly FROM item:new;
