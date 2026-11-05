/**
[env]
imports = ["reproductions/perm_predicate_runtime_block_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:test" }

[test]
reason = """
	A PERMISSIONS guard that reaches a write through a user-defined function
	body is refused where the write is reached, and must not write: a `select`
	guard, evaluated with permission enforcement disabled
	(GHSA-66r2-5gwj-gxm2), and an `update` guard alike, since no PERMISSIONS
	clause may have an effect. Each function writes only after its guard is
	defined, so the definition-time check accepted both guards.
"""

# 0: SELECT evaluates the select guard. It errors; the exact wording is wrapped
# differently per planner config ("Failed to check permission: ..."), so only
# assert that an error occurred.
[[test.results]]
error = true

# 1: UPDATE evaluates the update guard, whose write is refused the same way.
[[test.results]]
error = true

# 2: neither guard wrote.
[[test.results]]
value = "[]"
*/

-- 0: reading triggers the select guard -> its write is refused.
SELECT * FROM readable;

-- 1: writing triggers the update guard -> its write is refused.
UPDATE writable:1 SET n = 1;

-- 2: neither `audit:reader` nor `audit:writer` is present.
SELECT * FROM audit ORDER BY id;
