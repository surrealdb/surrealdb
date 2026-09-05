/**
[env.capabilities]
deny-functions = ["math::max", "time::now"]

[test]
reason = "Plan-time constant folding must not execute a function the caller's capabilities deny"

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
