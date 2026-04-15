/**
[env]
namespace = true
database = true
imports = ["reproductions/7208_reference_created_on_permission_fail_import.surql"]
auth = { namespace = "test", database = "test", access = "user_access", rid = "user:test" }

[test]
reason = "References must not be created when the CREATE permission check fails (WHERE false)"
issue = 7208

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '[{ "<~b": [] }]'
*/

-- As record user: create b referencing a:test should be denied (where false)
CREATE b CONTENT { a: a:test };

-- b should not exist
SELECT value id FROM b;

-- a:test should have no back-references from b
SELECT <~b FROM a:test;
