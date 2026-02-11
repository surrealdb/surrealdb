/**
[test]
reason = "Test EXPLAIN with GROUP BY and GROUP ALL aggregation"

[[test.results]]
value = "'OK'"

[[test.results]]
value = ''''Aggregate [ctx: Db] [mode: GROUP ALL]\n└────> Scan [ctx: Db] [source: sale]\n''''

[[test.results]]
value = ''''Aggregate [ctx: Db] [by: region]\n└────> Scan [ctx: Db] [source: sale]\n''''

[[test.results]]
value = ''''Sort [ctx: Db] [order_by: total DESC]\n└────> Aggregate [ctx: Db] [by: region]\n       └────> Scan [ctx: Db] [source: sale]\n''''
*/

-- Setup test data
{
	CREATE sale:1 SET region = 'NA', amount = 100;
	CREATE sale:2 SET region = 'NA', amount = 200;
	CREATE sale:3 SET region = 'EU', amount = 150;
	CREATE sale:4 SET region = 'EU', amount = 250;
	CREATE sale:5 SET region = 'APAC', amount = 300;
	RETURN "OK";
};

-- EXPLAIN GROUP ALL with count
EXPLAIN SELECT count() AS total FROM sale GROUP ALL;

-- EXPLAIN GROUP BY a single field
EXPLAIN SELECT region, count() AS cnt, math::sum(amount) AS total FROM sale GROUP BY region;

-- EXPLAIN GROUP BY with ORDER BY on the aggregated result
EXPLAIN SELECT region, math::sum(amount) AS total FROM sale GROUP BY region ORDER BY total DESC;
