/**
[env]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }
imports = ["language/permissions/dml/dml_permissions_setup.surql"]

[test]
reason = "Record user: SELECT on table with PERMISSIONS FOR select FULL returns all records"

[[test.results]]
value = "[{ id: select_full:1, owner: user:1, val: 'a' }, { id: select_full:2, owner: user:2, val: 'b' }]"

*/
SELECT * FROM select_full ORDER BY id;
