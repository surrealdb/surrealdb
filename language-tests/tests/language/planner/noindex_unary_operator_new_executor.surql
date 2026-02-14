/**
[test]
reason = "Test NOINDEX hint and unsupported unary operator fallback (new executor)"

[[test.results]]
value = "NONE"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 0, batches: 0}\n└────> Scan [ctx: Db] [source: `table`, predicate: ! param.subparam] {rows: 0, batches: 0}\n         actual: [access_method: TableScan, direction: Forward]\n\nTotal rows: 0''''

[[test.results]]
value = ''''Project [ctx: Db] {rows: 0, batches: 0}\n└────> Scan [ctx: Db] [source: `table`, predicate: ! param.subparam] {rows: 0, batches: 0}\n         actual: [access_method: TableScan, direction: Forward]\n\nTotal rows: 0''''
*/

DEFINE TABLE table;
EXPLAIN ANALYZE SELECT * FROM table WITH NOINDEX WHERE !param.subparam;
EXPLAIN ANALYZE SELECT * FROM table WHERE !param.subparam;
