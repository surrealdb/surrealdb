/**
[env]
imports = ["reproductions/perm_predicate_runtime_block_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:test" }

[test]
reason = """
	A `select` PERMISSIONS guard that reaches a write through a user-defined
	function body is blocked at runtime, and must not write
	(GHSA-66r2-5gwj-gxm2). The `create`/`update`/`delete` guards carry no such
	frame: they are reached from a statement that is already writing, so their
	side effects run.

	The block lives only at runtime. Nothing about these guards is refused when
	they are defined, because a call's body may take its writing branch only on
	arguments this guard never supplies.
"""

# 0: SELECT evaluates the select guard on the streaming path. It errors; the
# exact wording is wrapped differently per planner config ("Failed to check
# permission: ..."), so only assert that an error occurred.
[[test.results]]
error = true

# 1: UPDATE evaluates the update guard, whose write is permitted, so the
# statement succeeds.
[[test.results]]
value = "[{ id: writable:1, n: 1 }]"

# 2: only the update guard wrote. The select guard's write was refused.
[[test.results]]
value = "[{ hit: true, id: audit:writer }]"
*/

-- 0: reading triggers the select guard -> its write is refused.
SELECT * FROM readable;

-- 1: writing triggers the update guard -> its write runs.
UPDATE writable:1 SET n = 1;

-- 2: `audit:reader` must be absent; `audit:writer` must be present.
SELECT * FROM audit ORDER BY id;
