/**

[env]
imports = ["./field_permissions_import.surql"]

auth = { namespace = "test", database = "test", access = "account", rid = "user:bla" }

[test]

[[test.results]]
value = "[{ id: data:1 }]"

*/
UPDATE data:1 SET public = private;
