/**
[env.capabilities]
deny-functions = ["math::max"]

[test]
reason = "A record-id literal is inert according to its key: a scalar key just builds a value, while a generated or expression-bearing key must survive an eliminating fold"

[[test.results]]
value = "[{ a: 5, id: t:1 }]"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/

CREATE t:1 SET a = 5;

-- A key that carries an expression evaluates it, so a denied call inside one
-- must reach evaluation rather than be discarded with the operand.
SELECT * FROM t WHERE t:[math::max([1, 2])] IN [];
SELECT * FROM t WHERE t:[math::max([1, 2])] = t:1;

-- Scalar and range keys build a value and dispatch nothing, so these still
-- collapse; `t:rand()` is kept because it draws a fresh key per evaluation.
SELECT * FROM t WHERE t:1 IN [];
SELECT * FROM t WHERE t:abc IN [];
SELECT * FROM t WHERE t:1..9 IN [];
SELECT * FROM t WHERE t:rand() IN [];
