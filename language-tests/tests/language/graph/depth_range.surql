/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test range depth recursion using .{min..max} syntax"

# 0
[[test.results]]
value = "[person:ceo]"

# 1
[[test.results]]
value = "[person:ceo]"

# 2
[[test.results]]
value = "[person:vp_eng]"

# 3
[[test.results]]
value = "[person:ceo]"

# 4
[[test.results]]
value = "[person:bob, person:ceo, person:alice, person:dana, person:charlie]"
*/

-- 0: 1 to 4 levels up the management chain
person:alice.{1..4}->reports_to->person;

-- 1: At least 2 levels (unbounded upper)
person:alice.{2..}->reports_to->person;

-- 2: Up to 3 levels (unbounded lower means from 1)
person:alice.{..3}->reports_to->person;

-- 3: Unbounded traversal (traverse until dead end)
person:alice.{..}->reports_to->person;

-- 4: Range on knows edges (bounded to avoid cycles)
person:alice.{1..3}->knows->person;

