/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test diamond patterns (alice -> bob -> dana AND alice -> charlie -> dana)"

[[test.results]]
value = "[[person:ceo], [person:bob, person:alice, person:bob], [person:bob, person:alice, person:ceo], [person:bob, person:charlie, person:alice], [person:bob, person:charlie, person:dana], [person:bob, person:dana, person:charlie]]"

[[test.results]]
value = "[person:bob, person:dana]"

[[test.results]]
value = "[person:alice, person:bob, person:dana]"

[[test.results]]
value = "[person:bob, person:dana]"

[[test.results]]
value = "[project:database]"

[[test.results]]
value = "[[project:api, project:database], [project:auth, project:database], [project:api, project:auth, project:database]]"
*/

-- +path shows all paths (including diamond branches)
person:alice.{..3+path}->knows->person;

-- Shortest path to dana (will find 2-hop path)
person:alice.{..+shortest=person:dana}->knows->person;

-- Shortest path with inclusive
person:alice.{..+shortest=person:dana+inclusive}->knows->person;

-- Bounded shortest
person:alice.{..3+shortest=person:dana}->knows->person;

-- Project dependencies also have diamond-like patterns
project:frontend.{..4}->depends_on->project;

-- +path for dependencies
project:frontend.{..4+path}->depends_on->project;

