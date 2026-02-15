/**
[test]
reason = "Test EXPLAIN with multiple FROM sources and expression sources"

[[test.results]]
value = "'OK'"

[[test.results]]
value = ''''Project [ctx: Db]
└────> Union [ctx: Db]
       ├────> TableScan [ctx: Db] [table: cat, direction: Forward]
       └────> TableScan [ctx: Db] [table: dog, direction: Forward]
''''

[[test.results]]
value = ''''Project [ctx: Db]
└────> SourceExpr [ctx: Rt] [expr: [1, 2, 3]]
''''

[[test.results]]
value = ''''Project [ctx: Db]
└────> SourceExpr [ctx: Rt] [expr: <array>  0 .. 5]
''''
*/

-- Setup test data
{
	CREATE cat:1 SET name = 'Whiskers', kind = 'cat';
	CREATE dog:1 SET name = 'Rex', kind = 'dog';
	CREATE dog:2 SET name = 'Fido', kind = 'dog';
	RETURN "OK";
};

-- EXPLAIN SELECT from multiple tables (union)
EXPLAIN SELECT * FROM cat, dog;

-- EXPLAIN SELECT from an array expression
EXPLAIN SELECT * FROM [1, 2, 3];

-- EXPLAIN SELECT from a parameter/range expression
EXPLAIN SELECT * FROM <array> 0..5;
