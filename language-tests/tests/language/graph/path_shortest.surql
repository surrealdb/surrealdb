/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test +shortest modifier for finding shortest path to target"

# 0
[[test.results]]
value = "[person:lead_infra, person:dir_platform, person:vp_eng, person:ceo]"

# 1
[[test.results]]
value = "[person:lead_infra, person:dir_platform, person:vp_eng]"

# 2
[[test.results]]
value = "[person:bob, person:dana]"

# 3
[[test.results]]
value = "[person:bob]"

# 4
[[test.results]]
value = "[[person:lead_infra, person:dir_platform, person:vp_eng]]"

# 5
[[test.results]]
value = "[[person:lead_infra, person:dir_platform]]"
*/

-- 0: Shortest path from alice to CEO via reports_to
person:alice.{..+shortest=person:ceo}->reports_to->person;

-- 1: Shortest path from alice to VP of engineering
person:alice.{..+shortest=person:vp_eng}->reports_to->person;

-- 2: Shortest path via knows edges (alice to dana)
person:alice.{..+shortest=person:dana}->knows->person;

-- 3: Shortest path to bob via knows (direct connection)
person:alice.{..+shortest=person:bob}->knows->person;

-- 4: Bounded shortest path (max 3 hops)
person:alice.{..3+shortest=person:ceo}->reports_to->person;

-- 5: Bounded shortest path (max 2 hops) - may not find target
person:alice.{..2+shortest=person:ceo}->reports_to->person;

