/**
[env.capabilities]
deny-functions = ["math::max"]

[test]
reason = "An eliminating fold must not discard a call whose body is opaque at plan time, because an allowed call can still reach a denied one"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

*/

DEFINE FUNCTION fn::wrapper() { RETURN math::max([1, 2]); };
CREATE t:1 SET a = 5;

-- Control: the wrapper is allowed, but calling it reaches the denied built-in
-- in its body, so evaluation raises the denial.
RETURN fn::wrapper();

-- The same call as the sole operand of a collapsing fold. Matching the call
-- against its own capability name would report `fn::wrapper` as allowed and
-- let the fold discard it, hiding the denial its body would have raised.
-- Nothing at plan time can see into that body, so the subtree is kept.
SELECT * FROM t WHERE fn::wrapper() IN [];
SELECT * FROM t WHERE fn::wrapper() > 0 AND false;
SELECT * FROM t WHERE fn::wrapper() > 0 OR true;

-- Reached through an operand the short-circuit does evaluate.
SELECT * FROM t WHERE fn::wrapper() > 0;
