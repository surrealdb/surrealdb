/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test +collect modifier handling cycles gracefully"

[[test.results]]
value = "[person:bob, person:ceo, person:alice, person:charlie, person:dana]"

[[test.results]]
value = "[person:alice, person:bob, person:ceo, person:charlie, person:dana]"

[[test.results]]
value = "[{ name: 'Alice Zhang', network: ['Bob Martinez', 'Elena Rodriguez', 'Alice Zhang', 'Dana Lee', 'Charlie Brown'] }, { name: 'Bob Martinez', network: ['Alice Zhang', 'Charlie Brown', 'Dana Lee', 'Bob Martinez', 'Elena Rodriguez', 'Charlie Brown', 'Alice Zhang', 'Dana Lee'] }, { name: 'Charlie Brown', network: ['Alice Zhang', 'Charlie Brown', 'Dana Lee', 'Alice Zhang', 'Dana Lee'] }]"

[[test.results]]
value = "[person:bob, person:ceo, person:alice, person:charlie, person:dana]"
*/

-- Bounded collect through cycle
person:alice.{..6+collect}->knows->person;

-- Collect with inclusive (includes alice)
person:alice.{..6+collect+inclusive}->knows->person;

-- Multiple starting points, same cycle
SELECT name, @.{..3}(->knows->person).name AS network FROM person:alice, person:bob, person:charlie;

-- Combine bounded depth with collect for safety
person:alice.{..10+collect}->knows->person;

