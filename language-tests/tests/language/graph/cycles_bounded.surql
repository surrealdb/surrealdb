/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test bounded cycle traversal (alice -> bob -> charlie -> alice)"

[[test.results]]
value = "[person:bob, person:ceo, person:alice, person:dana, person:charlie]"

[[test.results]]
value = "[person:bob, person:ceo]"

[[test.results]]
value = "[person:alice, person:charlie, person:dana]"

[[test.results]]
value = "[person:alice, person:charlie, person:dana, person:bob, person:ceo, person:charlie, person:alice, person:dana, person:bob, person:ceo, person:alice, person:dana, person:charlie, person:bob, person:ceo, person:charlie, person:alice, person:charlie, person:dana, person:alice, person:dana]"

[[test.results]]
value = "[person:dir_product]"

[[test.results]]
value = "[person:lead_frontend]"
*/

-- Bounded traversal through cycle (will repeat if depth allows)
person:alice.{..3}->knows->person;

-- Single hop (no cycle yet)
person:alice.{1}->knows->person;

-- Two hops (approaches cycle)
person:alice.{2}->knows->person;

-- Traverse with depth limit to prevent infinite loop
person:alice.{..6}->knows->person;

-- Directors have bidirectional knows (another cycle pattern)
person:dir_platform.{..3}->knows->person;

-- Leadership knows cycle
person:lead_infra.{..3}->knows->person;

