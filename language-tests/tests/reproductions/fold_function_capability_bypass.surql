/**
[env.capabilities]
deny-functions = ["math::max", "time::now"]

[test]
reason = "Plan-time constant folding must not execute a function the caller's capabilities deny, nor discard a subtree that runtime evaluation would have reached"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, at: d'2020-01-01T00:00:00Z', id: t:1 }]"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

[[test.results]]
error = "Function 'time::now' is not allowed to be executed"

[[test.results]]
value = "[{ a: 5, at: d'2020-01-01T00:00:00Z', id: t:1 }]"

[[test.results]]
error = "Function 'math::max' is not allowed to be executed"

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
value = "[{ a: 5, at: d'2020-01-01T00:00:00Z', id: t:1 }]"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

*/

DEFINE TABLE t SCHEMALESS;
DEFINE INDEX ta ON t FIELDS a;
CREATE t:1 SET a = 5, at = d"2020-01-01T00:00:00Z";

-- Control: a denied call evaluated directly.
RETURN math::max([1, 2]);

-- A denied call whose arguments are all literals: foldable in shape, so the
-- folder must refuse it and leave the call to fail at evaluation.
SELECT * FROM t WHERE a > math::max([1, 2]);

-- A denied call nested inside an allowed one. Bottom-up folding reaches the
-- inner call first; refusing it leaves the outer call unfoldable too.
SELECT * FROM t WHERE a > math::floor(math::max([1, 2]));

-- time::now() folds by deliberate exception to the determinism gate, so it
-- needs the capability check on its own arm.
SELECT * FROM t WHERE at < time::now();

-- An allowed function still folds, and the folded literal still drives the
-- index range.
SELECT * FROM t WHERE a > math::floor(1.5);

-- A denied call inside an operand that a parent fold discards outright.
-- `x INSIDE []` is provably false whatever `x` is, and an AND/OR
-- short-circuit drops the operand it does not return — so refusing to invoke
-- the call is not enough on its own; the subtree holding it has to survive
-- planning, or no physical expression is ever built to raise the denial.
SELECT * FROM t WHERE math::max([1, 2]) IN [];
SELECT * FROM t WHERE math::max([1, 2]) > 0 AND false;
SELECT * FROM t WHERE math::max([1, 2]) > 0 OR true;

-- The same collapses still apply when nothing in the operand is denied, so
-- the empty-scan optimisation is unaffected.
SELECT * FROM t WHERE a IN [];
SELECT * FROM t WHERE math::floor(1.5) IN [];

-- Short-circuit ordering decides which operand needs protecting. `AND`/`OR`
-- evaluate left to right, so `false AND x` and `true OR x` never reach `x`:
-- collapsing them discards a subtree that would not have run, which is what
-- evaluation itself does, so these still fold even though `x` is denied.
SELECT * FROM t WHERE false AND math::max([1, 2]) > 0;
SELECT * FROM t WHERE true OR math::max([1, 2]) > 0;
RETURN false AND math::max([1, 2]) > 0;
RETURN true OR math::max([1, 2]) > 0;
