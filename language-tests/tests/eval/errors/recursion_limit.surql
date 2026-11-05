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
reason = "A self-referential eval (a function whose body evaluates a query that calls the function again) is bounded by expression-nesting depth against max_computation_depth, and errors with a computation-depth error instead of overflowing the stack. The planner tracks the nesting depth and eval continues the count when it re-plans its query string, so the native stack cannot grow without limit. It must not crash."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"
*/

DEFINE FUNCTION fn::recurse() { RETURN eval::surql("RETURN fn::recurse()"); };
RETURN fn::recurse();
