/**
[test]
reason = "Test EXPLAIN with SPLIT clause"

[[test.results]]
value = "'OK'"

[[test.results]]
value = ''''Project [ctx: Db]
    Split [ctx: Db] [on: tags]
        TableScan [ctx: Db] [table: post, direction: Forward]
''''

[[test.results]]
value = ''''Project [ctx: Db]
    SortByKey [ctx: Db] [sort_keys: views DESC]
        Split [ctx: Db] [on: tags]
            TableScan [ctx: Db] [table: post, direction: Forward]
''''
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
