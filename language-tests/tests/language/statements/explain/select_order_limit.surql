/**
[test]
reason = "Test EXPLAIN with ORDER BY, LIMIT, and START clauses"

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
"Project [ctx: Db]
└────> SortByKey [ctx: Db] [sort_keys: val ASC]
       └────> Scan [ctx: Db] [source: record]
"'''

[[test.results]]
value = '''
"Project [ctx: Db]
└────> SortByKey [ctx: Db] [sort_keys: val DESC]
       └────> Scan [ctx: Db] [source: record]
"'''

[[test.results]]
value = '''
"Project [ctx: Db]
└────> Scan [ctx: Db] [source: record, limit: 5]
"'''

[[test.results]]
value = '''
"Project [ctx: Db]
└────> Scan [ctx: Db] [source: record, limit: 3, offset: 2]
"'''

[[test.results]]
value = '''"Project [ctx: Db]
└────> Limit [ctx: Db] [limit: 2, offset: 1]
       └────> SortTopKByKey [ctx: Db] [sort_keys: val DESC, limit: 3]
              └────> Scan [ctx: Db] [source: record]
"'''
*/

-- Setup test data
{
	FOR $i IN 1..11 {
		CREATE record CONTENT { val: $i };
	};
	RETURN "OK";
};

-- EXPLAIN ORDER BY ascending
EXPLAIN SELECT * FROM record ORDER BY val ASC;

-- EXPLAIN ORDER BY descending
EXPLAIN SELECT * FROM record ORDER BY val DESC;

-- EXPLAIN with LIMIT only
EXPLAIN SELECT * FROM record LIMIT 5;

-- EXPLAIN with LIMIT and START
EXPLAIN SELECT * FROM record LIMIT 3 START 2;

-- EXPLAIN combining ORDER BY, LIMIT, and START
EXPLAIN SELECT * FROM record ORDER BY val DESC LIMIT 2 START 1;
