/**
[env]
imports = ["datasets/graph.surql"]

[test]
reason = "Test forward edge traversal (->) patterns"

[[test.results]]
value = "[person:lead_infra]"

[[test.results]]
value = "[person:bob, person:ceo]"

[[test.results]]
value = "['Priya Patel']"

[[test.results]]
value = "[{ id: person:alice, manager: ['Priya Patel'], name: 'Alice Zhang' }]"

[[test.results]]
value = "[{ id: person:alice, knows: ['Bob Martinez', 'Elena Rodriguez'], name: 'Alice Zhang' }, { id: person:bob, knows: ['Alice Zhang', 'Charlie Brown', 'Dana Lee'], name: 'Bob Martinez' }]"

[[test.results]]
value = "[{ manager: ['Priya Patel'], name: 'Alice Zhang' }, { manager: ['Priya Patel'], name: 'Bob Martinez' }, { manager: ['Alex Thompson'], name: 'Charlie Brown' }, { manager: ['Alex Thompson'], name: 'Dana Lee' }]"
*/

-- Single hop forward: who does alice report to?
person:alice->reports_to->person;

-- Single hop forward: who does alice know?
person:alice->knows->person;

-- Forward traversal with field access
person:alice->reports_to->person.name;

-- Forward traversal in SELECT
SELECT id, name, ->reports_to->person.name AS manager FROM person:alice;

-- Multiple traversals from a set of records
SELECT id, name, ->knows->person.name AS knows FROM person:alice, person:bob;

-- Use @ to refer to current record in traversal
SELECT name, @->reports_to->person.name AS manager FROM person WHERE level = 5;

