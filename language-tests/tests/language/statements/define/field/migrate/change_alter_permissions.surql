/**
[env]
imports = ["language/statements/define/field/migrate/change_alter_permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "Change 21: a PERMISSIONS-only ALTER FIELD over rows stored before a migration applies to those rows at once, and the permission judges the migrated value."

[[test.results]]
value = "[{ id: doc:1 }, { id: doc:2, n: 7 }]"

[[test.results]]
value = "[NONE, 7]"

[[test.results]]
value = "[NONE, NONE]"

[[test.results]]
value = "[{ id: doc:2 }]"

[[test.results]]
value = "7"

[[test.results]]
value = "NONE"
*/
SELECT * FROM doc ORDER BY id;
SELECT VALUE n FROM doc ORDER BY id;
SELECT VALUE s FROM doc ORDER BY id;
SELECT id FROM doc WHERE n > 5;
doc:2.n;
doc:1.n;
