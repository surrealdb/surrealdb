/**
[test]
reason = "Test EXPLAIN with basic SELECT queries: table scan, predicates, specific records, and SELECT VALUE"

[[test.results]]
value = "'OK'"

[[test.results]]
value = ''''Project [ctx: Db]
└────> TableScan [ctx: Db] [table: item, direction: Forward]
''''

[[test.results]]
value = ''''Project [ctx: Db]
└────> Filter [ctx: Db] [predicate: score > 15]
       └────> TableScan [ctx: Db] [table: item, direction: Forward, predicate: score > 15]
''''

[[test.results]]
value = ''''Project [ctx: Db]
└────> DynamicScan [ctx: Db] [source: item:1]
''''

[[test.results]]
value = ''''ProjectValue [ctx: Db] [expr: name]
└────> TableScan [ctx: Db] [table: item, direction: Forward]
''''

[[test.results]]
value = ''''Project [ctx: Db]
└────> TableScan [ctx: Db] [table: item, direction: Forward]
''''
*/

-- Setup test data
{
	CREATE item:1 SET name = 'Alpha', score = 10;
	CREATE item:2 SET name = 'Beta', score = 20;
	CREATE item:3 SET name = 'Gamma', score = 30;
	RETURN "OK";
};

-- EXPLAIN a basic full table scan
EXPLAIN SELECT * FROM item;

-- EXPLAIN a scan with a WHERE predicate
EXPLAIN SELECT * FROM item WHERE score > 15;

-- EXPLAIN selecting a specific record
EXPLAIN SELECT * FROM item:1;

-- EXPLAIN with SELECT VALUE (project value)
EXPLAIN SELECT VALUE name FROM item;

-- EXPLAIN with field projection and alias
EXPLAIN SELECT name, score * 2 AS doubled FROM item;
