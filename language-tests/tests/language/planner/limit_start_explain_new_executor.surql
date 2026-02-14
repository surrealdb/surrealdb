/**
[env]
timeout = 5000

[test]
reason = "Test LIMIT and START with EXPLAIN plans and ORDER BY (new executor)"

[[test.results]]
value = "[]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 8, batches: 1}
└────> Limit [ctx: Db] [limit: 10, offset: 2] {rows: 8, batches: 1}
       └────> TableScan [ctx: Db] [table: item, direction: Forward, limit: 10, offset: 2] {rows: 10, batches: 1}

Total rows: 8''''

[[test.results]]
value = "[]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 8, batches: 1}
└────> Limit [ctx: Db] [limit: 10, offset: 2] {rows: 8, batches: 1}
       └────> TableScan [ctx: Db] [table: item2, direction: Forward, limit: 10, offset: 2] {rows: 10, batches: 1}

Total rows: 8''''

[[test.results]]
value = "[1, 2, 3, 4, 5]"

[[test.results]]
value = "[5, 6, 7, 8, 9]"

[[test.results]]
value = "[8, 9, 10]"
*/

CREATE |item:100| RETURN NONE;
EXPLAIN ANALYZE SELECT * FROM item LIMIT 10 START 2;

CREATE |item2:100| RETURN NONE;
EXPLAIN ANALYZE SELECT * FROM item2 ORDER BY id LIMIT 10 START 2;

-- LIMIT/START from array (deterministic)
SELECT * FROM [1,2,3,4,5,6,7,8,9,10] LIMIT 5 START 0;
SELECT * FROM [1,2,3,4,5,6,7,8,9,10] LIMIT 5 START 4;
SELECT * FROM [1,2,3,4,5,6,7,8,9,10] LIMIT 5 START 7;
