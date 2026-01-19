/**
[test]
reason = "Test GROUP BY with NONE values in the grouping field"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ category: NONE, count: 3, total: 80f }, { category: 'A', count: 2, total: 25f }, { category: 'B', count: 1, total: 30f }]"

[[test.results]]
value = "[{ avg: 26.666666666666668f, category: NONE }]"

*/

-- Setup data
{
	CREATE record:1 SET category = 'A', value = 10;
	CREATE record:2 SET category = NONE, value = 20;
	CREATE record:3 SET category = 'A', value = 15;
	CREATE record:4 SET category = NONE, value = 25;
	CREATE record:5 SET category = 'B', value = 30;
	CREATE record:6 SET value = 35;
	RETURN "OK";
};

-- Test grouping with NONE values
SELECT category, COUNT() as count, math::sum(value) as total FROM record GROUP BY category;

-- Test grouping field with all NONE
SELECT category, math::mean(value) as avg FROM record WHERE category = NONE OR category IS NONE GROUP BY category;
