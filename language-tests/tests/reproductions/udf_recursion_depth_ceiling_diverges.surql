/**
[env]
namespace = true
database = true
backend = ["mem"]

[test]
reason = "A user-defined function recursion is bounded by `max_computation_depth`, and each call consumes one unit of that budget. A body that recurses 115 levels completes against the budget of 120."

[[test.results]]
value = "NONE"

[[test.results]]
value = "50"

[[test.results]]
value = "115"
*/

DEFINE FUNCTION fn::down($n: int) { IF $n <= 0 { RETURN 0 }; RETURN fn::down($n - 1) + 1; };
RETURN fn::down(50);
RETURN fn::down(115);
