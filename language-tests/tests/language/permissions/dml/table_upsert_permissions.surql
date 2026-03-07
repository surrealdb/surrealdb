/**
[env]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }
imports = ["language/permissions/dml/dml_permissions_setup.surql"]

[test]
reason = "Record user: UPSERT respects table CREATE/UPDATE permissions"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: create_full:ups, owner: user:1, val: 'new' }]"

[[test.results]]
value = "[{ id: update_full:1, owner: user:1, val: 'upserted' }]"

[[test.results]]
value = "[]"

*/
-- New record on create_none: denied (create NONE)
UPSERT create_none:ups SET val = 'no', owner = user:1;
-- New record on create_full: allowed (create FULL)
UPSERT create_full:ups SET val = 'new', owner = user:1;
-- Existing record on update_full: allowed (update FULL)
UPSERT update_full:1 SET val = 'upserted';
-- Existing record on update_none: denied (update NONE)
UPSERT update_none:1 SET val = 'no';
