/**
[test]
reason = "Test EXPLAIN with multiple FROM sources and expression sources"

[[test.results]]
value = "'OK'"

[[test.results]]
value = ''''Project [ctx: Db]\n└────> Union [ctx: Db]\n       ├────> Scan [ctx: Db] [source: cat]\n       └────> Scan [ctx: Db] [source: dog]\n''''

[[test.results]]
value = ''''Project [ctx: Db]\n└────> SourceExpr [ctx: Rt] [expr: [1, 2, 3]]\n''''

[[test.results]]
value = ''''Project [ctx: Db]\n└────> SourceExpr [ctx: Rt] [expr: <array>  0 .. 5]\n''''
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
