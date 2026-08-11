/**
[env]
imports = ["reproductions/mutable_permissions_enabled_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:test" }

[test]
reason = """
	With the `mutable_permissions` capability enabled (the language-test runner
	turns experimental targets on by default), a create/update permission
	predicate that writes through a function executes the write at request time.
	A record user triggers the predicates (owner would bypass them). Contrast
	with permission_predicate_runtime_block, which asserts the block when the
	write hides behind `eval` and no capability opens it up.
"""

# 0: CREATE fires the create-permission predicate, which writes audit:created.
[[test.results]]
value = "[{ id: thing:1, n: 0 }]"

# 1: the create predicate's write executed.
[[test.results]]
value = "[{ hit: true, id: audit:created }]"

# 2: UPDATE fires the update-permission predicate, which writes audit:updated.
[[test.results]]
value = "[{ id: thing:1, n: 1 }]"

# 3: both predicate writes are now present.
[[test.results]]
value = "[{ hit: true, id: audit:created }, { hit: true, id: audit:updated }]"
*/

CREATE thing:1 SET n = 0;
SELECT * FROM audit:created;
UPDATE thing:1 SET n = 1;
SELECT * FROM audit ORDER BY id;
