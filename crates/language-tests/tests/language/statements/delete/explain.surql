/**
[test]

[[test.results]]
value = "[{ id: person:test, name: 'Tester' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ detail: { table: 'test' }, operation: 'Iterate Table' }, { detail: { type: 'MemoryOrdered' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ detail: { table: 'test' }, operation: 'Iterate Table' }, { detail: { type: 'MemoryOrdered' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: person:test, name: 'Tester' }]"

*/

CREATE person:test SET name = 'Tester';
DEFINE INDEX idx ON person FIELDS name;
DELETE person:test WHERE name = 'Tester' EXPLAIN;
DELETE person:test EXPLAIN;
SELECT * FROM person;
