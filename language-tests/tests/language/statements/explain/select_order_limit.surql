/**
[test]
reason = "Test EXPLAIN with ORDER BY, LIMIT, and START clauses"

[[test.results]]
value = "'OK'"

[[test.results]]
value = ''''Project [ctx: Db]\n└────> SortByKey [ctx: Db] [sort_keys: val ASC]\n       └────> Scan [ctx: Db] [source: record]\n''''

[[test.results]]
value = ''''Project [ctx: Db]\n└────> SortByKey [ctx: Db] [sort_keys: val DESC]\n       └────> Scan [ctx: Db] [source: record]\n''''

[[test.results]]
value = ''''Project [ctx: Db]\n└────> Scan [ctx: Db] [source: record, limit: 5]\n''''

[[test.results]]
value = ''''Project [ctx: Db]\n└────> Scan [ctx: Db] [source: record, limit: 3, offset: 2]\n''''

[[test.results]]
value = ''''Project [ctx: Db]\n└────> Limit [ctx: Db] [limit: 2, offset: 1]\n       └────> SortByKey [ctx: Db] [sort_keys: val DESC]\n              └────> Scan [ctx: Db] [source: record]\n''''
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
