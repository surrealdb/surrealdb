/**
[test]
reason = "Test GROUP BY with ORDER BY clause for sorting grouped results"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ region: 'east', total: 480f }, { region: 'north', total: 250f }, { region: 'south', total: 450f }]"

[[test.results]]
value = "[{ region: 'east', total: 480f }, { region: 'north', total: 250f }, { region: 'south', total: 450f }]"

[[test.results]]
value = "[{ region: 'east', total: 480f }, { region: 'north', total: 250f }, { region: 'south', total: 450f }]"

[[test.results]]
value = "[{ count: 2, region: 'east' }, { count: 2, region: 'north' }, { count: 2, region: 'south' }]"

*/

-- Setup data
{
	CREATE sale:1 SET region = 'north', amount = 100;
	CREATE sale:2 SET region = 'south', amount = 200;
	CREATE sale:3 SET region = 'north', amount = 150;
	CREATE sale:4 SET region = 'east', amount = 300;
	CREATE sale:5 SET region = 'south', amount = 250;
	CREATE sale:6 SET region = 'east', amount = 180;
	RETURN "OK";
};

-- Test ORDER BY on group field
SELECT region, math::sum(amount) as total FROM sale GROUP BY region ORDER BY region;

-- Test ORDER BY on aggregate field ascending
SELECT region, math::sum(amount) as total FROM sale GROUP BY region ORDER BY total;

-- Test ORDER BY on aggregate field descending
SELECT region, math::sum(amount) as total FROM sale GROUP BY region ORDER BY total DESC;

-- Test ORDER BY with COUNT
SELECT region, COUNT() as count FROM sale GROUP BY region ORDER BY count DESC;
