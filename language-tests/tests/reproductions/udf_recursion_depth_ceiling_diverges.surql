/**
[env]
namespace = true
database = true
backend = ["mem"]
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = "The two engines bound a user-defined function recursion at very different depths, and this pins the streaming side. A body that recurses 115 levels completes on the streaming engine, whose budget is one unit of `max_computation_depth` (120) per call; the same query on the legacy engine errors well before 50 levels, because a level there costs several units of `Options::dive` — the call, the block, and the nested expression each take one. Neither number is wrong, but they are not the same number, so a recursion depth between the two limits succeeds or fails depending on which engine ran it. Asserted for the streaming strategies alone; the legacy ceiling is what it has always been and is not worth pinning to a specific level while that engine is on its way out."

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
