/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test fixed depth recursion using .{n} syntax"

[[test.results]]
value = "[person:lead_infra]"

[[test.results]]
value = "[person:dir_platform]"

[[test.results]]
value = "[person:vp_eng]"

[[test.results]]
value = "[person:ceo]"

[[test.results]]
value = "[person:alice, person:charlie, person:dana]"
*/

-- Exactly 1 level up management chain
person:alice.{1}->reports_to->person;

-- Exactly 2 levels up (alice -> lead_infra -> dir_platform)
person:alice.{2}->reports_to->person;

-- Exactly 3 levels up (alice -> lead_infra -> dir_platform -> vp_eng)
person:alice.{3}->reports_to->person;

-- Exactly 4 levels up to CEO
person:alice.{4}->reports_to->person;

-- Fixed depth on knows edges
person:alice.{2}->knows->person;

