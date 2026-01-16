/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test forward edge traversal (->) patterns"

# 0
[[test.results]]
value = "[person:lead_infra]"

# 1
[[test.results]]
value = "[person:bob, person:ceo]"

# 2
[[test.results]]
value = "['Priya Patel']"

# 3
[[test.results]]
value = "[{ id: person:alice, manager: ['Priya Patel'], name: 'Alice Zhang' }]"

# 4
[[test.results]]
value = "[{ id: person:alice, knows: ['Bob Martinez', 'Elena Rodriguez'], name: 'Alice Zhang' }, { id: person:bob, knows: ['Alice Zhang', 'Charlie Brown', 'Dana Lee'], name: 'Bob Martinez' }]"

# 5
[[test.results]]
value = "[{ manager: ['Priya Patel'], name: 'Alice Zhang' }, { manager: ['Priya Patel'], name: 'Bob Martinez' }, { manager: ['Alex Thompson'], name: 'Charlie Brown' }, { manager: ['Alex Thompson'], name: 'Dana Lee' }]"
*/

-- 0: Single hop forward: who does alice report to?
person:alice->reports_to->person;

-- 1: Single hop forward: who does alice know?
person:alice->knows->person;

-- 2: Forward traversal with field access
person:alice->reports_to->person.name;

-- 3: Forward traversal in SELECT
SELECT id, name, ->reports_to->person.name AS manager FROM person:alice;

-- 4: Multiple traversals from a set of records
SELECT id, name, ->knows->person.name AS knows FROM person:alice, person:bob;

-- 5: Use @ to refer to current record in traversal
SELECT name, @->reports_to->person.name AS manager FROM person WHERE level = 5;

