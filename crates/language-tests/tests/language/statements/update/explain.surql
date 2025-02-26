/**
[test]

[[test.results]]
value = "[{ id: person:test, name: 'foo' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ detail: { thing: person:test }, operation: 'Iterate Thing' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: person:test, name: 'foo' }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'person' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: person:test, name: 'foo' }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx', operator: '=', value: 'bar' }, table: 'person' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: person:test, name: 'foo' }]"

[[test.results]]
value = "[{ id: person:test, name: 'bar' }]"

[[test.results]]
value = "[{ id: person:test, name: 'bar' }]"

*/

CREATE person:test SET name = 'foo';
DEFINE INDEX idx ON person FIELDS name;
UPDATE person:test SET name = 'bar' EXPLAIN ;
SELECT * FROM person;
UPDATE person SET name = 'bar' EXPLAIN;
SELECT * FROM person;
UPDATE person SET name = 'bar' WHERE name = 'bar' EXPLAIN;
SELECT * FROM person;
UPDATE person SET name = 'bar' WHERE name = 'foo';
SELECT * FROM person;