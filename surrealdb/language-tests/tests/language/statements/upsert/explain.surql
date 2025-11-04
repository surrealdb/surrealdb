/**
[test]

[[test.results]]
value = "[{ id: person:test, name: 'foo' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ detail: { thing: person:test }, operation: 'Iterate Defer' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: person:test, name: 'foo' }]"

[[test.results]]
value = "[{ detail: { table: 'person' }, operation: 'Iterate Yield' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

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
UPSERT person:test SET name = 'bar' EXPLAIN ;
SELECT * FROM person;
UPSERT person SET name = 'bar' EXPLAIN;
SELECT * FROM person;
UPSERT person SET name = 'bar' WHERE name = 'bar' EXPLAIN;
SELECT * FROM person;
UPSERT person SET name = 'bar' WHERE name = 'foo';
SELECT * FROM person;