/**
[env.capabilities]
deny-experimental = ["files"]
deny-functions = ["math::max"]

[test]
reason = "type::file, file::bucket and file::key are pure, so they carry no context of their own and are foldable; the experimental gate the legacy dispatcher applies to them must be applied by whoever invokes them"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
error = "There was a problem running the type::file() function. Experimental feature files is not enabled"

[[test.results]]
error = "There was a problem running the type::file() function. Experimental feature files is not enabled"

[[test.results]]
error = "There was a problem running the type::file() function. Experimental feature files is not enabled"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

*/

CREATE t:1 SET a = 5;

-- Folding executes the call, so the gate is checked before it is folded.
SELECT * FROM t WHERE type::file('bucket', 'key') = 'x';
SELECT * FROM t WHERE type::file('bucket', 'key') IN [];

-- And outside any fold, where the built-in physical expression runs it.
RETURN type::file('bucket', 'key');

-- The gate is applied where the legacy dispatcher applies it, after the
-- arguments, so an argument's own error is the one that surfaces.
RETURN type::file(math::max([1, 2]), 'key');
