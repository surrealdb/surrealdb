/**
[env]

[test]
reason = "Test EXPLAIN with SPLIT clause. A `SPLIT` divides the row the statement returns, so the split sits above the projection and the sort above the split — the order the statement runs them in. The projection is therefore below the sort here, where every other shape projects last."

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Split [ctx: Db] [targets: tags]
        SelectProject [ctx: Db] [projections: *]
            TableScan [ctx: Db] [table: post, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Sort [ctx: Db] [sort_keys: views DESC]
        Split [ctx: Db] [targets: tags]
            SelectProject [ctx: Db] [projections: *]
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
