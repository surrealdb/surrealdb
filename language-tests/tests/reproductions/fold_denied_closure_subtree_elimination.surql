/**
[env.capabilities]
deny-functions = ["math::max"]

[test]
reason = "A closure call is an Expr::Postfix, not an Expr::FunctionCall, so a fold must not discard it on the strength of matching call nodes"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

*/

CREATE t:1 SET a = 5;

-- A closure-valued parameter survives parameter resolution as `Expr::Param`,
-- because substituting its literal form would drop the captures. Invoking it
-- is `Expr::Postfix`, so nothing in the operand is a call node at all, yet
-- evaluating it reaches the denied built-in in the closure body.
LET $f = |$x: int| math::max([1, 2]);

SELECT * FROM t WHERE ($f)(0) IN [];
SELECT * FROM t WHERE ($f)(0) > 0 AND false;
SELECT * FROM t WHERE ($f)(0) > 0;
