/**
[env]
imports = ["reproductions/planned_source_enforced_once_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	A closure body is compiled without a principal and resolves the table it
	reads where it is invoked, so a record user reading a table through a
	closure sees exactly the rows and fields a direct `SELECT` shows it: the
	table permission removes `t:3` and the field guard cuts `hidden` from the
	record the caller does not own.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ hidden: 'mine', id: t:1, n: 1, owner: user:reader, secret: 'x' }, { id: t:2, n: 2, owner: user:other, secret: 'x' }]"

[[test.results]]
value = "[t:1]"
*/

LET $f = || (SELECT * FROM t ORDER BY id);
RETURN $f();
RETURN array::map([1], |$x| (SELECT id FROM t WHERE n = $x)[0].id);
