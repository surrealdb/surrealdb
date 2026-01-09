/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test +shortest modifier for finding shortest path to target"

[[test.results]]
value = "[person:lead_infra, person:dir_platform, person:vp_eng, person:ceo]"

[[test.results]]
value = "[person:lead_infra, person:dir_platform, person:vp_eng]"

[[test.results]]
value = "[person:bob, person:dana]"

[[test.results]]
value = "[person:bob]"

[[test.results]]
value = "[[person:lead_infra, person:dir_platform, person:vp_eng]]"

[[test.results]]
value = "[[person:lead_infra, person:dir_platform]]"
*/

-- Shortest path from alice to CEO via reports_to
person:alice.{..+shortest=person:ceo}->reports_to->person;

-- Shortest path from alice to VP of engineering
person:alice.{..+shortest=person:vp_eng}->reports_to->person;

-- Shortest path via knows edges (alice to dana)
person:alice.{..+shortest=person:dana}->knows->person;

-- Shortest path to bob via knows (direct connection)
person:alice.{..+shortest=person:bob}->knows->person;

-- Bounded shortest path (max 3 hops)
person:alice.{..3+shortest=person:ceo}->reports_to->person;

-- Bounded shortest path (max 2 hops) - may not find target
person:alice.{..2+shortest=person:ceo}->reports_to->person;

