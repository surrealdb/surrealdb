/**
[env]
imports = ["language/statements/define/field/computed/hidden_is_never_evaluated_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	A value the principal cannot see is never evaluated (register #2). The
	record user may select `shown` only: `hidden` and `bad` deny every caller,
	and `admin_only` admits an administrator, which this user is not. Reading
	the table derives `shown` and nothing else, so the sequence advances by one
	per row per statement and the body of `bad`, which raises when it runs, is
	never reached.

	A record reached through a link is read the same way, whether it is
	dereferenced in a projection or fetched: the linked record carries `shown`
	alone and draws once. The final draw is consecutive with the reads above.
"""

# 0, 1: one draw per row per statement.
[[test.results]]
value = "[{ id: doc:1, shown: 0 }]"

[[test.results]]
value = "[{ id: doc:1, shown: 1 }]"

# 2: a dereference in a projection.
[[test.results]]
value = "[{ link: { id: doc:1, shown: 2 } }]"

# 3: a FETCH.
[[test.results]]
value = "[{ id: refs:1, link: { id: doc:1, shown: 3 } }]"

# 4: the next draw follows the four above.
[[test.results]]
value = "4"
*/

SELECT * FROM doc;
SELECT * FROM doc;
SELECT link.* FROM refs;
SELECT * FROM refs FETCH link;
RETURN sequence::nextval('s');
