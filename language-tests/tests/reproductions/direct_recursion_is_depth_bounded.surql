/**
[env]
namespace = true
database = true
# Engine-level depth bound — storage-agnostic, so validate on `mem` only. The
# recursion deliberately descends to the depth budget before erroring, which is
# instant on `mem` but does a catalog read per level on the disk backends.
backend = ["mem"]

[test]
reason = "A user-defined function that calls itself directly, with no `eval::surql` between the levels, must reach the computation-depth limit and error rather than overflow the native stack. The streaming engine compiles a recursive edge to the plan of the body it sits in, so the recursion re-enters one shared plan and nothing is re-planned per level; the only thing standing between it and an unbounded native recursion is the depth budget each call consumes (`ExecutionContext::dive`). The eval-based recursion tests cover the re-planning boundary instead, where the planner's own re-entry count applies, so neither of them exercises this guard."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"
*/

-- Self-recursion: the edge is the whole returned expression.
DEFINE FUNCTION fn::loop($n: int) { RETURN fn::loop($n + 1); };
RETURN fn::loop(0);

-- Mutual recursion: the edge names a function further up the path, so it
-- resolves to that frame's shared plan rather than its own.
DEFINE FUNCTION fn::pong($n: int) { RETURN fn::ping($n + 1); };
DEFINE FUNCTION fn::ping($n: int) { RETURN fn::pong($n + 1); };
RETURN fn::ping(0);
