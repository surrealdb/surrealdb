/**
[env]
imports = ["language/statements/define/field/computed/computed_under_where_permission_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	A computed field the reader cannot see is never evaluated, whether a
	select permission of NONE or a WHERE permission hides it: the permission is
	judged first for the row and the COMPUTED body runs only when it admits.
	The record user may select `shown` only, and `admin_only` admits an
	administrator, which this user is not. Its body calls `fn::r`, which
	writes, and `bad`'s raises whenever it runs, so a body that ran would fail
	the read with the COMPUTED frame's refusal. Each read returns the row
	without the hidden fields and raises nothing.

	A record reached through a link is read the same way, whether it is
	dereferenced in a projection or fetched.

	`gated` is guarded by the same stored fact and throws through a stored
	function when its body runs: for a reader it denies, the body does not run,
	so the read raises nothing: the row lacks the field, and a projection that
	names it reads NONE.
"""

# 0, 1: a read of the table derives `shown` and nothing hidden.
[[test.results]]
value = "[{ id: doc:1, shown: 'shown' }]"

[[test.results]]
value = "[{ id: doc:1, shown: 'shown' }]"

# 2: a dereference in a projection.
[[test.results]]
value = "[{ link: { id: doc:1, shown: 'shown' } }]"

# 3: a FETCH.
[[test.results]]
value = "[{ id: refs:1, link: { id: doc:1, shown: 'shown' } }]"

# 4, 5: a denied body that would throw does not run, even when named.
[[test.results]]
value = "[{ id: gate:1 }]"

[[test.results]]
value = "[{ gated: NONE }]"
*/

SELECT * FROM doc;
SELECT * FROM doc;
SELECT link.* FROM refs;
SELECT * FROM refs FETCH link;
SELECT * FROM gate;
SELECT gated FROM gate;
