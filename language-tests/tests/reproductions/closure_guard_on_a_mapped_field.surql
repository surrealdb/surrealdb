/**
[env]
namespace = true
database = true
imports = ["reproductions/closure_guard_on_a_mapped_field_import.surql"]

signin = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "alice@surrealdb.com",
	pass: "secret",
}"""

[test]
reason = """
A field permission on a mapped path (`items[*].secret`) is judged against the
record, not against the array element the path widens to. Guarded by a closure
invocation the guard does not contain, that still has to hold: `t:1` refuses at
the record and allows at the element, and the element must not win.

`$pred()` is what `SECURITY_GUIDE.md` calls an invocation whose body is nowhere
in the expression, which must be read as demanding the whole row. `t:2` is the
control, refusing at the element and allowing at the record, so the guard is
shown to be live rather than merely closed.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ allowed: false, id: t:1, items: [{ allowed: true, shown: 1 }] }]"

[[test.results]]
value = "[{ allowed: true, id: t:2, items: [{ allowed: false, secret: 'SHOWN', shown: 2 }] }]"
*/

LET $pred = || allowed;
SELECT * FROM t:1;
SELECT * FROM t:2;
