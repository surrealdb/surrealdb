/**
[test]

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ '<~b': [] }]"

[env]
imports = ["reproductions/7208_reference_create_permission_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:test" }
*/

-- Attempt to create b as a record user (should fail due to WHERE false permission)
CREATE b CONTENT { a: a:test };

-- b should not exist
SELECT VALUE id FROM b;

-- No reference should have been created on a
SELECT <~b FROM a;
