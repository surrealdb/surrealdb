/**
[env]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }
imports = ["language/permissions/field/field_permissions_setup.surql"]

[test]
reason = "Field-level UPDATE permission: 'readonly' field with FOR update NONE is preserved when record user updates"

[[test.results]]
value = "[{ id: item:1, locked: 'preset', name: 'changed', owner: user:1, readonly: 'fixed', secret: 'hidden' }]"

[[test.results]]
value = "[{ name: 'changed', readonly: 'fixed' }]"

*/
-- Update should change name but not readonly (readonly has FOR update NONE)
UPDATE item:1 SET name = 'changed', readonly = 'should_not_change';
SELECT name, readonly FROM item:1;
