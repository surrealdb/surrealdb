/**
[test]

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: a:test, id: b:test }]"

[[test.results]]
value = "[{ '<~b': [] }]"

[env]
imports = ["reproductions/7208_reference_upsert_permission_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:test" }
*/

-- Attempt to upsert b:test as a record user (should fail due to WHERE false permission)
UPSERT b:test CONTENT { a: a:test2 };

-- b:test should still have the original reference to a:test
SELECT * FROM b:test;

-- a:test2 should have no ghost reference
SELECT <~b FROM a:test2;
