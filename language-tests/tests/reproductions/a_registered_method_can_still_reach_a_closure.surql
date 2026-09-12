/**
[env]
namespace = true
database = true
imports = ["./a_registered_method_can_still_reach_a_closure_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:probe" }

[test]
reason = """
	A closure reached at runtime must be judged above the field-permission
	layer, because what its body reads cannot be told from the expression: a
	predicate below the layer sees the stored `secret`, and reporting whether
	it matched leaks the field through row membership.

	A method call is one of the ways to reach one. `.name(args)` on a value
	dispatches on the receiver's type, and a registered name with no
	implementation for that type degrades to invoking a closure stored in a
	field of that name. So being registered does not make a call safe: `get`
	is registered for files alone, and `$o.get()` on an object carrying a
	`get` closure runs it.

	Only a name an *object* receiver dispatches for is exempt, an object being
	the only value that can carry the field such a closure would be stored in.

	The second query is the control for the already-opaque half — an
	unregistered name, which has always been withheld — and the third is what
	the caller may actually see, which names no `secret` at all.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:1, ok: 1 }, { id: t:2, ok: 2 }]"
*/

-- A registered name with no implementation for an object receiver.
LET $o = { get: || secret = 'yes' };
SELECT ok FROM t WHERE $o.get();

-- Control: an unregistered name, reached the same way.
LET $p = { check: || secret = 'yes' };
SELECT ok FROM t WHERE $p.check();

-- Control: the rows as this caller may read them.
SELECT * FROM t;
