/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test +path modifier for collecting all paths"

[[test.results]]
value = "[[person:lead_infra, person:dir_platform, person:vp_eng, person:ceo]]"

[[test.results]]
value = "[[person:ceo], [person:bob, person:alice, person:bob], [person:bob, person:alice, person:ceo], [person:bob, person:charlie, person:alice], [person:bob, person:charlie, person:dana], [person:bob, person:dana, person:charlie]]"

[[test.results]]
value = "[[person:lead_infra, person:dir_platform, person:vp_eng]]"
*/

-- All paths from alice through reports_to edges
person:alice.{..+path}->reports_to->person;

-- All paths through knows edges (bounded to avoid infinite paths)
person:alice.{..3+path}->knows->person;

-- Bounded path collection
person:alice.{1..3+path}->reports_to->person;

