/**
[test]
reason = "Test GROUP BY with mixed data types in the grouping field"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ count: 1, total: 35f, type: false }, { count: 1, total: 25f, type: true }, { count: 2, total: 50f, type: 123 }, { count: 2, total: 25f, type: 'string' }]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ category: 1, total: 450f }, { category: 2, total: 250f }]"

*/

-- Setup data
{
	CREATE mixed:1 SET type = 'string', value = 10;
	CREATE mixed:2 SET type = 123, value = 20;
	CREATE mixed:3 SET type = 'string', value = 15;
	CREATE mixed:4 SET type = true, value = 25;
	CREATE mixed:5 SET type = 123, value = 30;
	CREATE mixed:6 SET type = false, value = 35;
	RETURN "OK";
};

-- Test grouping with mixed types (string, number, boolean)
SELECT type, COUNT() as count, math::sum(value) as total FROM mixed GROUP BY type;

-- Test grouping with mixed numeric types
{
	CREATE numbers:1 SET category = 1, amount = 100;
	CREATE numbers:2 SET category = 1.0, amount = 200;
	CREATE numbers:3 SET category = 1, amount = 150;
	CREATE numbers:4 SET category = 2, amount = 250;
	RETURN "OK";
};

SELECT category, math::sum(amount) as total FROM numbers GROUP BY category;
