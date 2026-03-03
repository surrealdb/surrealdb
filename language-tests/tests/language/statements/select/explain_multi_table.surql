/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test EXPLAIN and EXPLAIN FULL with multiple FROM tables"

[[test.results]]
value = "[{ id: person:tobie, name: 'Tobie' }]"

[[test.results]]
value = "[{ id: person:jaime, name: 'Jaime' }]"

[[test.results]]
value = "[{ id: software:surreal, name: 'SurrealDB' }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'person' }, operation: 'Iterate Table' }, { detail: { direction: 'forward', table: 'software' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'person' }, operation: 'Iterate Table' }, { detail: { direction: 'forward', table: 'software' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 3 }, operation: 'Fetch' }]"
*/

CREATE person:tobie SET name = 'Tobie';
CREATE person:jaime SET name = 'Jaime';
CREATE software:surreal SET name = 'SurrealDB';
SELECT * FROM person,software EXPLAIN;
SELECT * FROM person,software EXPLAIN FULL;
