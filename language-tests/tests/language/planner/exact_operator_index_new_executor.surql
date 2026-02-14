/**
[test]
reason = "Test exact equality operator (==) uses index same as (=) (new executor)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ b: true, i: 1, id: t:1 }]"

[[test.results]]
value = "[{ b: false, i: 2, id: t:2 }]"

[[test.results]]
value = "[{ b: true, i: 1, id: t:1 }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 1, batches: 1}\n└────> Scan [ctx: Db] [source: t, predicate: b == true] {rows: 1, batches: 1}\n         actual: [access_method: IndexScan, index: idx, access: Equality(true), direction: Forward]\n\nTotal rows: 1''''

[[test.results]]
value = "[{ b: false, i: 2, id: t:2 }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 1, batches: 1}\n└────> Scan [ctx: Db] [source: t, predicate: i == 2] {rows: 1, batches: 1}\n         actual: [access_method: IndexScan, index: uniq, access: Equality(2), direction: Forward]\n\nTotal rows: 1''''
*/

DEFINE INDEX idx ON TABLE t COLUMNS b;
DEFINE INDEX uniq ON TABLE t COLUMNS i;
CREATE t:1 set b = true, i = 1;
CREATE t:2 set b = false, i = 2;
SELECT * FROM t WHERE b == true;
EXPLAIN ANALYZE SELECT * FROM t WHERE b == true;
SELECT * FROM t WHERE i == 2;
EXPLAIN ANALYZE SELECT * FROM t WHERE i == 2;
