/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test fixed depth recursion using .{n} syntax"

# 0
[[test.results]]
value = "[person:lead_infra]"

# 1
[[test.results]]
value = "[person:dir_platform]"

# 2
[[test.results]]
value = "[person:vp_eng]"

# 3
[[test.results]]
value = "[person:ceo]"

# 4
[[test.results]]
value = "[person:alice, person:charlie, person:dana]"
*/

-- 0: Exactly 1 level up management chain
person:alice.{1}->reports_to->person;

-- 1: Exactly 2 levels up (alice -> lead_infra -> dir_platform)
person:alice.{2}->reports_to->person;

-- 2: Exactly 3 levels up (alice -> lead_infra -> dir_platform -> vp_eng)
person:alice.{3}->reports_to->person;

-- 3: Exactly 4 levels up to CEO
person:alice.{4}->reports_to->person;

-- 4: Fixed depth on knows edges
person:alice.{2}->knows->person;

