/**
[test]
reason = "Test SELECT VALUE with negation and boolean NOT expressions (new executor)"

[[test.results]]
value = "[{ boolean: true, id: thing:a, number: 5 }]"

[[test.results]]
value = "[{ boolean: false, id: thing:b, number: -5 }]"

[[test.results]]
value = "[-5, 5]"

[[test.results]]
value = "[false, true]"

[[test.results]]
value = ''''ProjectValue [ctx: Db] [expr: ! boolean] {rows: 2, batches: 1}\n└────> Scan [ctx: Db] [source: thing] {rows: 2, batches: 1}\n         actual: [access_method: TableScan, direction: Forward]\n\nTotal rows: 2''''
*/

CREATE thing:a SET number = 5, boolean = true;
CREATE thing:b SET number = -5, boolean = false;
SELECT VALUE -number FROM thing;
SELECT VALUE !boolean FROM thing;
EXPLAIN ANALYZE SELECT VALUE !boolean FROM thing;
