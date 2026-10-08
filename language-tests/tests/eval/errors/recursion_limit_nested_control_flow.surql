/**
[env]
namespace = true
database = true
# Engine-level depth bound — storage-agnostic, so validate on `mem` only. The
# recursion deliberately descends to `max_computation_depth` (~120 levels of
# parse+plan+execute) before erroring, which is instant on `mem` but does ~120
# catalog reads per level on the disk backends (slow enough to blow the CI job
# timeout); those backends add no extra coverage for this check.
backend = ["mem"]

[env.capabilities]
allow-eval-query = ["system"]

[test]
reason = "Self-referential eval recursion must stay bounded even when each level nests through control-flow operators (IF / FOR) and binding-carrying blocks, not just a bare RETURN. These are planned as deferred operators (IfElsePlan/ForeachPlan/SequencePlan) that re-plan their bodies at runtime; each records the planner's nesting depth and continues the count across the re-entry, so the recursion reaches max_computation_depth and errors cleanly instead of overflowing the native stack."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"
*/

-- The body wraps the recursive eval in an IF, and the eval string wraps the
-- recursive call in an IF + binding-carrying block — exercising the deferred
-- IfElse / Sequence operators on both sides of the re-entry.
DEFINE FUNCTION fn::test($d: int) {
	IF true {
		RETURN eval::surql("{ LET $n = $d + 1; IF true { RETURN fn::test($n) } }", { d: $d });
	}
};
IF true { RETURN fn::test(0); };
