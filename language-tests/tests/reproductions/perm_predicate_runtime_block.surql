/**
[env]
imports = ["reproductions/perm_predicate_runtime_block_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:test" }

[env.capabilities]
allow-eval-query = ["record", "system"]

[test]
reason = "A permission predicate that writes via a custom-function body must be blocked at runtime on both the streaming SELECT path and the legacy UPDATE path, and must not create the row (GHSA-66r2-5gwj-gxm2). The write hides behind `eval` because a resolvable function-call write is refused at definition time."

# 0: SELECT — streaming path. It errors; the exact wording is wrapped
# differently per planner config ("Failed to check permission: ..."), so only
# assert that an error occurred.
[[test.results]]
error = true

# 1: UPDATE — legacy compute path, stable error wording across configs.
[[test.results]]
error = "A PERMISSIONS clause cannot contain a statement that modifies data"

# 2: the predicate's write was blocked, so audit is empty.
[[test.results]]
value = "[]"
*/

-- 0: SELECT triggers the SELECT-permission predicate (streaming path) -> blocked.
SELECT * FROM victim;

-- 1: UPDATE triggers the UPDATE-permission predicate (legacy path) -> blocked.
UPDATE victim:1 SET visible = false;

-- 2: the predicate must not have written to `audit`.
SELECT * FROM audit;
