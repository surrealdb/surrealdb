/**
[env]
imports = ["./records-10000-rule-planner.surql"]

[test]
run = false
reason = "The rule-planner records dataset with `created_at` and `age` declared, so neither column holds an array and an index over either answers in key order."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

DEFINE FIELD created_at ON record TYPE option<datetime>;
DEFINE FIELD age ON record TYPE option<int>;
