/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test WHERE with record ID field, AND conditions, and index usage with EXPLAIN"

[[test.results]]
value = "[{ id: person:tobie, name: 'Tobie' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ author: person:tobie, id: post:1 }]"

[[test.results]]
value = "[{ author: person:tobie, id: post:2 }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'author', operator: '=', value: person:tobie }, table: 'post' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'author', operator: '=', value: person:tobie }, table: 'post' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 2 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ author: person:tobie, id: post:1 }, { author: person:tobie, id: post:2 }]"

[[test.results]]
value = "[{ genre: 'm', id: person2:tobie, name: 'Tobie' }]"

[[test.results]]
value = "[{ genre: 'm', id: person2:jaime, name: 'Jaime' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ detail: { plan: { index: 'person2_name', operator: '=', value: 'Tobie' }, table: 'person2' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ name: 'Tobie' }]"

[[test.results]]
value = "[{ genre: 'm', id: person3:tobie, name: 'Tobie' }]"

[[test.results]]
value = "[{ genre: 'm', id: person3:jaime, name: 'Jaime' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ detail: { plan: { index: 'person3_name', operator: '=', value: 'Jaime' }, table: 'person3' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ name: 'Jaime' }]"
*/

-- Record field with standard index
CREATE person:tobie SET name = 'Tobie';
DEFINE INDEX author ON TABLE post COLUMNS author;
CREATE post:1 SET author = person:tobie;
CREATE post:2 SET author = person:tobie;
SELECT * FROM post WHERE author = person:tobie EXPLAIN;
SELECT * FROM post WHERE author = person:tobie EXPLAIN FULL;
SELECT * FROM post WHERE author = person:tobie;

-- AND with standard index
CREATE person2:tobie SET name = 'Tobie', genre='m';
CREATE person2:jaime SET name = 'Jaime', genre='m';
DEFINE INDEX person2_name ON TABLE person2 COLUMNS name;
SELECT name FROM person2 WHERE name = 'Tobie' AND genre = 'm' EXPLAIN;
SELECT name FROM person2 WHERE name = 'Tobie' AND genre = 'm';

-- AND with unique index
CREATE person3:tobie SET name = 'Tobie', genre='m';
CREATE person3:jaime SET name = 'Jaime', genre='m';
DEFINE INDEX person3_name ON TABLE person3 COLUMNS name UNIQUE;
SELECT name FROM person3 WHERE name = 'Jaime' AND genre = 'm' EXPLAIN;
SELECT name FROM person3 WHERE name = 'Jaime' AND genre = 'm';
