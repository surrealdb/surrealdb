/**
[test]
reason = "Test EXPLAIN ANALYZE with basic SELECT queries - executes and shows metrics"

[[test.results]]
value = "'OK'"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 3}
└────> TableScan [ctx: Db] [table: item, direction: Forward] {rows: 3}

Total rows: 3''''

[[test.results]]
value = ''''Project [ctx: Db] {rows: 2}
└────> TableScan [ctx: Db] [table: item, direction: Forward, predicate: score > 15] {rows: 2}

Total rows: 2''''

[[test.results]]
value = ''''ProjectValue [ctx: Db] [expr: name] {rows: 3}
└────> TableScan [ctx: Db] [table: item, direction: Forward] {rows: 3}

Total rows: 3''''
*/

-- Setup test data
{
	CREATE item:1 SET name = 'Alpha', score = 10;
	CREATE item:2 SET name = 'Beta', score = 20;
	CREATE item:3 SET name = 'Gamma', score = 30;
	RETURN "OK";
};

-- EXPLAIN ANALYZE a basic full table scan
EXPLAIN ANALYZE SELECT * FROM item;

-- EXPLAIN ANALYZE with a WHERE predicate
EXPLAIN ANALYZE SELECT * FROM item WHERE score > 15;

-- EXPLAIN ANALYZE with SELECT VALUE
EXPLAIN ANALYZE SELECT VALUE name FROM item;
