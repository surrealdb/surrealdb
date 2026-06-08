/**
[test]
reason = "Top-level `LET $r = CREATE ...` falls back to the legacy compute path because the streaming planner returns PlannerUnsupported for DML. The fall-back must still produce the created record and bind it correctly."

[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

*/

LET $r = CREATE created:once SET v = 1;
RETURN $r[0].v;
