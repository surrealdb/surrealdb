/**
[test]
reason = "Test SELECT from an array of record IDs (DynamicScan array source)"

[[test.results]]
value = "[{ id: person:1, name: 'Alice' }]"

[[test.results]]
value = "[{ id: person:2, name: 'Bob' }]"

[[test.results]]
value = "[{ id: person:3, name: 'Charlie' }]"

[[test.results]]
value = "[{ id: person:1, name: 'Alice' }, { id: person:3, name: 'Charlie' }]"

[[test.results]]
error = "Specify a database to use"

*/

CREATE person:1 SET name = 'Alice';
CREATE person:2 SET name = 'Bob';
CREATE person:3 SET name = 'Charlie';

SELECT * FROM [person:1, person:3];
SELECT name FROM [person:2, person:1] ORDER BY name;
