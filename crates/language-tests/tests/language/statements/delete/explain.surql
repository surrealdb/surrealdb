/**
[test]

[[test.results]]
value = "[{ id: person:test, name: 'Tester' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ detail: { thing: person:test }, operation: 'Iterate Thing' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: person:test, name: 'Tester' }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'person' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: person:test, name: 'Tester' }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx', operator: '=', value: 'Tester' }, table: 'person' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: person:test, name: 'Tester' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/

CREATE person:test SET name = 'Tester';
DEFINE INDEX idx ON person FIELDS name;
DELETE person:test EXPLAIN;
SELECT * FROM person;
DELETE person EXPLAIN;
SELECT * FROM person;
DELETE person WHERE name = 'Tester' EXPLAIN;
SELECT * FROM person;
DELETE person WHERE name = 'Tester';
SELECT * FROM person;