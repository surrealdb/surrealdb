/**
[test]
reason = "Test EXPLAIN with SPLIT clause"

[[test.results]]
value = "'OK'"

[[test.results]]
value = ''''Project [ctx: Db]\n└────> Split [ctx: Db] [on: tags]\n       └────> Scan [ctx: Db] [source: post]\n''''

[[test.results]]
value = ''''Project [ctx: Db]\n└────> SortByKey [ctx: Db] [sort_keys: views DESC]\n       └────> Split [ctx: Db] [on: tags]\n              └────> Scan [ctx: Db] [source: post]\n''''
*/

-- Setup test data
{
	CREATE post:1 SET tags = ['rust', 'db'], views = 100;
	CREATE post:2 SET tags = ['rust', 'wasm'], views = 200;
	CREATE post:3 SET tags = ['db', 'sql'], views = 50;
	RETURN "OK";
};

-- EXPLAIN SPLIT on an array field
EXPLAIN SELECT * FROM post SPLIT tags;

-- EXPLAIN SPLIT with ORDER BY
EXPLAIN SELECT * FROM post SPLIT tags ORDER BY views DESC;
