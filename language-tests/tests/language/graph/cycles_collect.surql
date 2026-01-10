/**
[env]
imports = ["datasets/graph.surql"]
# This is set higher just to ensure that the TiKV backend doesn't exceed the timeout.
timeout = 5000

[test]
reason = "Test +collect modifier handling cycles gracefully"

# 0
[[test.results]]
value = "[person:bob, person:ceo, person:alice, person:charlie, person:dana]"

# 1
[[test.results]]
value = "[person:alice, person:bob, person:ceo, person:charlie, person:dana]"

# 2
[[test.results]]
value = "[{ name: 'Alice Zhang', network: ['Bob Martinez', 'Elena Rodriguez', 'Alice Zhang', 'Dana Lee', 'Charlie Brown'] }, { name: 'Bob Martinez', network: ['Alice Zhang', 'Charlie Brown', 'Dana Lee', 'Bob Martinez', 'Elena Rodriguez', 'Charlie Brown', 'Alice Zhang', 'Dana Lee'] }, { name: 'Charlie Brown', network: ['Alice Zhang', 'Charlie Brown', 'Dana Lee', 'Alice Zhang', 'Dana Lee'] }]"

# 3
[[test.results]]
value = "[person:bob, person:ceo, person:alice, person:charlie, person:dana]"
*/

-- 0: Bounded collect through cycle
person:alice.{..6+collect}->knows->person;

-- 1: Collect with inclusive (includes alice)
person:alice.{..6+collect+inclusive}->knows->person;

-- 2: Multiple starting points, same cycle
SELECT name, @.{..3}(->knows->person).name AS network FROM person:alice, person:bob, person:charlie;

-- 3: Combine bounded depth with collect for safety
person:alice.{..10+collect}->knows->person;

