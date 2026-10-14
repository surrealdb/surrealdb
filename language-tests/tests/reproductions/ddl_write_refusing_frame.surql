/**
[env]
imports = ["reproductions/ddl_write_refusing_frame_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:test" }

[test]
reason = """
	A schema statement writes the catalog, so the two frames that refuse a write
	refuse it too: a `select` PERMISSIONS guard, which is evaluated with
	permission enforcement disabled (GHSA-66r2-5gwj-gxm2), and a COMPUTED body,
	which runs on every read of the field under the definer's auth.

	Both refusals happen when the statement is reached: each callee is
	redefined with its schema statement after the clause that calls it is
	defined, so the definition-time check accepted the clause and the read
	reaches the runtime frame.

	What this file holds is that a schema statement in either frame is refused
	with the same message. Each frame is enforced twice — the operator that plans a
	schema statement refuses ahead of the statement's work, and the evaluator
	that performs the catalog write refuses off its own options — so removing
	either one alone leaves these assertions passing. The operator's own
	refusal is pinned by `exec::operators::ddl`, which asserts that `execute`
	itself fails and no stream is produced.
"""

# 0: reading evaluates the select guard, whose schema statement is refused.
[[test.results]]
error = "The PERMISSIONS clause must not have effects: a schema statement changes the schema"

# 1: reading the computed field evaluates its body, whose schema statement is
# refused the same way, and names its own frame.
[[test.results]]
error = "The COMPUTED clause must not have effects: a schema statement changes the schema"
*/

-- 0: the select guard's schema statement is refused.
SELECT * FROM guarded;

-- 1: the computed body's schema statement is refused.
SELECT computed FROM computed_holder;
