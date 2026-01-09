/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test range depth recursion using .{min..max} syntax"

[[test.results]]
value = "[person:ceo]"

[[test.results]]
value = "[person:ceo]"

[[test.results]]
value = "[person:vp_eng]"

[[test.results]]
value = "[person:ceo]"

[[test.results]]
value = "[person:bob, person:ceo, person:alice, person:dana, person:charlie]"
*/

-- 1 to 4 levels up the management chain
person:alice.{1..4}->reports_to->person;

-- At least 2 levels (unbounded upper)
person:alice.{2..}->reports_to->person;

-- Up to 3 levels (unbounded lower means from 1)
person:alice.{..3}->reports_to->person;

-- Unbounded traversal (traverse until dead end)
person:alice.{..}->reports_to->person;

-- Range on knows edges (bounded to avoid cycles)
person:alice.{1..3}->knows->person;

