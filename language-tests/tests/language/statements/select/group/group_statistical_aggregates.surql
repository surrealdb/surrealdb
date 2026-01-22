/**
[test]
reason = "Test GROUP BY with statistical aggregate functions (median, mode, stddev)"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ category: 'A', median: 15f }, { category: 'B', median: 32.5f }]"

[[test.results]]
value = "[{ category: 'A', stddev: 5f }, { category: 'B', stddev: 6.454972243679028f }]"

[[test.results]]
value = "[{ category: 'A', variance: 25f }, { category: 'B', variance: 41.666666666666664f }]"

[[test.results]]
value = "[{ category: 'A', mean: 15f, median: 15f, stddev: 5f }, { category: 'B', mean: 32.5f, median: 32.5f, stddev: 6.454972243679028f }]"

*/

-- Setup data
{
	CREATE stats:1 SET category = 'A', value = 10;
	CREATE stats:2 SET category = 'A', value = 20;
	CREATE stats:3 SET category = 'A', value = 15;
	CREATE stats:4 SET category = 'B', value = 25;
	CREATE stats:5 SET category = 'B', value = 30;
	CREATE stats:6 SET category = 'B', value = 35;
	CREATE stats:7 SET category = 'B', value = 40;
	RETURN "OK";
};

-- Test median with GROUP BY
SELECT category, math::median(value) as median FROM stats GROUP BY category;

-- Test standard deviation with GROUP BY
SELECT category, math::stddev(value) as stddev FROM stats GROUP BY category;

-- Test variance with GROUP BY
SELECT category, math::variance(value) as variance FROM stats GROUP BY category;

-- Test multiple statistical aggregates together
SELECT category, math::mean(value) as mean, math::median(value) as median, math::stddev(value) as stddev FROM stats GROUP BY category;
