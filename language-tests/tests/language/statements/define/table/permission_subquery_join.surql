/**
[env]
namespace = true
database = true
imports = ["language/statements/define/table/permission_subquery_join_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	A table permission of the form `$auth.id IN (SELECT VALUE who FROM grant
	WHERE …)` is planned as a semi join in the permission frame: the grant
	table, which the record user may not read directly, is read inside the
	permission as the per-row subquery reads it, and the caller sees exactly
	the documents a grant names, whether the subquery is correlated on the
	document or not, and with or without a predicate of the caller's own.
"""

[[test.results]]
value = "[doc:1, doc:3]"

[[test.results]]
value = "[doc:3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[note:1]"

[[test.results]]
value = "[]"
*/

SELECT VALUE id FROM doc ORDER BY id;
SELECT VALUE id FROM doc WHERE title != 'one' ORDER BY id;
SELECT VALUE id FROM doc:2;
SELECT VALUE id FROM note;
SELECT VALUE id FROM grant;
