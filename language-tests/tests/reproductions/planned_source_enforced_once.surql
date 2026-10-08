/**
[env]
imports = ["reproductions/planned_source_enforced_once_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	A record user reading a table with a conditional table permission and a
	field guard sees the rows the permission admits, with the guarded fields cut. Both
	declarations are enforced once, in the plan above
	the scan: the table permission removes `t:3`, and `hidden` is cut from
	the record the caller does not own. A predicate on the guarded field is
	judged over the row as the caller sees it, so it never matches a cut
	value.
"""

[[test.results]]
value = "[{ hidden: 'mine', id: t:1, n: 1, owner: user:reader, secret: 'x' }, { id: t:2, n: 2, owner: user:other, secret: 'x' }]"

[[test.results]]
value = "[{ hidden: 'mine', id: t:1, n: 1, owner: user:reader, secret: 'x' }, { id: t:2, n: 2, owner: user:other, secret: 'x' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:1 }]"

*/

SELECT * FROM t ORDER BY id;
SELECT * FROM t WHERE n > 0 ORDER BY id;
SELECT id FROM t WHERE hidden = 'theirs';
SELECT id FROM t WHERE hidden = 'mine';
