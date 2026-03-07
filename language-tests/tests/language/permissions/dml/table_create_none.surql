/**
[env]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }
imports = ["language/permissions/dml/dml_permissions_setup.surql"]

[test]
reason = "Record user: CREATE on table with PERMISSIONS FOR create NONE is silently denied"

[[test.results]]
value = "[]"

*/
CREATE create_none:new SET val = 'denied', owner = user:1;
