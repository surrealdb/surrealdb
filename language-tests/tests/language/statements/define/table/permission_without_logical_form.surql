/**
[env]
namespace = true
database = true
imports = ["language/statements/define/table/permission_without_logical_form_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	A stored `PERMISSIONS FOR select` guard whose shape the logical lowering
	does not express — a mock record set in scalar position — is still
	enforced: the analyzer
	leaves the table's source unresolved and the scan evaluates the stored
	clause itself, so the read is neither declined nor answered differently.
	The same holds for a field guard, and the guards compose: the table guard
	admits three rows and the field guard admits the value on two of them.
"""

# 0: the table guard admits `mock_guarded:1..3`; the field guard cuts `tag`
# from the row it does not admit.
[[test.results]]
value = "[{ id: mock_guarded:1, n: 1, tag: 'one' }, { id: mock_guarded:2, n: 2, tag: 'two' }, { id: mock_guarded:3, n: 3 }]"

# 1: a filtered read is judged by the same guard.
[[test.results]]
value = "[{ id: mock_guarded:3, n: 3 }]"

# 2: a direct record read outside the guard's set yields nothing.
[[test.results]]
value = "[]"

# 3: a table whose guards all have a logical form is unaffected by sharing
# the statement with one that does not.
[[test.results]]
value = "[{ id: plain:1, guarded: true, other: mock_guarded:1 }]"
*/

SELECT * FROM mock_guarded ORDER BY id;
SELECT * FROM mock_guarded WHERE n > 2;
SELECT * FROM mock_guarded:5;
SELECT * FROM plain WHERE other IN (SELECT VALUE id FROM mock_guarded WHERE n = 1);
