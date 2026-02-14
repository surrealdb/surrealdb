/**
[test]
reason = "Test EXPLAIN ANALYZE with multiple FROM tables (new executor)"

[[test.results]]
value = "[{ id: person:tobie, name: 'Tobie' }]"

[[test.results]]
value = "[{ id: person:jaime, name: 'Jaime' }]"

[[test.results]]
value = "[{ id: software:surreal, name: 'SurrealDB' }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 3, batches: 2}\n└────> Union [ctx: Db] {rows: 3, batches: 2}\n       ├────> Scan [ctx: Db] [source: person] {rows: 2, batches: 1}\n       │        actual: [access_method: TableScan, direction: Forward]\n       └────> Scan [ctx: Db] [source: software] {rows: 1, batches: 1}\n                actual: [access_method: TableScan, direction: Forward]\n\nTotal rows: 3''''
*/

CREATE person:tobie SET name = 'Tobie';
CREATE person:jaime SET name = 'Jaime';
CREATE software:surreal SET name = 'SurrealDB';
EXPLAIN ANALYZE SELECT * FROM person,software;
