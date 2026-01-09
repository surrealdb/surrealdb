/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "WIP: Test TIMEOUT on graph expressions (currently has parsing errors)"
wip = true
*/

-- BROKEN: TIMEOUT on graph expression doesn't parse
-- This will succeed because we set an upper bound
SELECT name, @.{..6}(->knows->person).name AS knows FROM person:alice TIMEOUT 1s;

-- This will timeout because we did not set an upper bound on a cyclic graph
SELECT name, @.{..}(->knows->person).name AS knows FROM person:alice TIMEOUT 1s;

-- Bounded depth should complete within timeout
person:alice.{..4}->knows->person TIMEOUT 2s;

-- Bounded collect should complete
person:alice.{..6+collect}->knows->person TIMEOUT 2s;

