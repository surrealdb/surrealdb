/**
[env.capabilities]
deny-functions = ["type::file", "math::max"]

[test]
reason = "type::file, file::bucket and file::key are pure, so they carry no context of their own and are foldable; a capability that denies one must be applied by whoever invokes it, the plan-time folder included"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
error = "Function 'type::file' is not allowed to be executed"

[[test.results]]
error = "Function 'type::file' is not allowed to be executed"

[[test.results]]
error = "Function 'type::file' is not allowed to be executed"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

*/

CREATE t:1 SET a = 5;

-- Folding executes the call, so the capability is checked before it is folded.
SELECT * FROM t WHERE type::file('bucket', 'key') = 'x';
SELECT * FROM t WHERE type::file('bucket', 'key') IN [];

-- And outside any fold, where the built-in physical expression runs it.
RETURN type::file('bucket', 'key');

-- The arguments are evaluated first, so a denied argument's error is the one
-- that surfaces.
RETURN type::file(math::max([1, 2]), 'key');
