/**

[env]
planner-strategy = ["compute-only"]

[test]
reason = "Ensure edge traversal in array FROM uses the lookup optimization and processes all elements"
issue = 7124

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:alice, name: 'Alice' }]"

[[test.results]]
value = "[{ id: person:bob, name: 'Bob' }]"

[[test.results]]
value = "[{ id: person:carol, name: 'Carol' }]"

[[test.results]]
value = "[{ id: knows:1, in: person:alice, out: person:bob }]"

[[test.results]]
value = "[{ id: person:bob, name: 'Bob' }]"

[[test.results]]
value = "[{ id: person:bob, name: 'Bob' }]"

[[test.results]]
value = "[{ id: person:bob, name: 'Bob' }, { id: person:carol, name: 'Carol' }]"

*/

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
CREATE person:alice SET name = 'Alice';
CREATE person:bob SET name = 'Bob';
CREATE person:carol SET name = 'Carol';
RELATE person:alice->knows:1->person:bob;
-- Non-array edge traversal
SELECT * FROM person:alice->knows->person;
-- Array edge traversal should produce the same result
SELECT * FROM [person:alice->knows->person];
-- Multi-element arrays must process all elements (not just the first)
SELECT * FROM [person:alice->knows->person, person:carol];
