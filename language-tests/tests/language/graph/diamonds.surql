/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test diamond patterns (alice -> bob -> dana AND alice -> charlie -> dana)"

# 0, 1
[[test.results]]
value = "[[person:ceo], [person:bob, person:alice, person:bob], [person:bob, person:alice, person:ceo], [person:bob, person:charlie, person:alice], [person:bob, person:charlie, person:dana], [person:bob, person:dana, person:charlie]]"
[[test.results]]
value = "[[person:alice, person:ceo], [person:alice, person:bob, person:alice, person:bob], [person:alice, person:bob, person:alice, person:ceo], [person:alice, person:bob, person:charlie, person:alice], [person:alice, person:bob, person:charlie, person:dana], [person:alice, person:bob, person:dana, person:charlie]]"

# 2, 3
[[test.results]]
value = "[person:bob, person:dana]"
[[test.results]]
value = "[person:alice, person:bob, person:dana]"

# 4, 5
[[test.results]]
value = "[person:bob, person:dana]"
[[test.results]]
value = "[person:alice, person:bob, person:dana]"

# 6
[[test.results]]
value = "[project:database]"

# 7
[[test.results]]
value = "[[project:api, project:database], [project:auth, project:database], [project:api, project:auth, project:database]]"
*/

-- +path shows all paths (including diamond branches)
-- 0, 1
person:alice.{..3+path}->knows->person;
person:alice.{..3+path+inclusive}->knows->person;

-- Shortest path to dana (will find 2-hop path)
-- 2, 3
person:alice.{..+shortest=person:dana}->knows->person;
person:alice.{..+shortest=person:dana+inclusive}->knows->person;

-- Bounded shortest
-- 4, 5
person:alice.{..3+shortest=person:dana}->knows->person;
person:alice.{..3+shortest=person:dana+inclusive}->knows->person;

-- Project dependencies also have diamond-like patterns
-- 6
project:frontend.{..4}->depends_on->project;

-- +path for dependencies
-- 7
project:frontend.{..4+path}->depends_on->project;

