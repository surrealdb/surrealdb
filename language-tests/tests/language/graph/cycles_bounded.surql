/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test bounded cycle traversal (alice -> bob -> charlie -> alice)"

# 0
[[test.results]]
value = "[person:bob, person:ceo, person:alice, person:dana, person:charlie]"

# 1
[[test.results]]
value = "[person:bob, person:ceo]"

# 2
[[test.results]]
value = "[person:alice, person:charlie, person:dana]"

# 3
[[test.results]]
value = "[person:alice, person:charlie, person:dana, person:bob, person:ceo, person:charlie, person:alice, person:dana, person:bob, person:ceo, person:alice, person:dana, person:charlie, person:bob, person:ceo, person:charlie, person:alice, person:charlie, person:dana, person:alice, person:dana]"

# 4
[[test.results]]
value = "[person:dir_product]"

# 5
[[test.results]]
value = "[person:lead_frontend]"
*/

-- 0: Bounded traversal through cycle (will repeat if depth allows)
person:alice.{..3}->knows->person;

-- 1: Single hop (no cycle yet)
person:alice.{1}->knows->person;

-- 2: Two hops (approaches cycle)
person:alice.{2}->knows->person;

-- 3: Traverse with depth limit to prevent infinite loop
person:alice.{..6}->knows->person;

-- 4: Directors have bidirectional knows (another cycle pattern)
person:dir_platform.{..3}->knows->person;

-- 5: Leadership knows cycle
person:lead_infra.{..3}->knows->person;

