/**
[test]
reason = "Test GROUP BY with nonexistent fields and aggregates over missing fields"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ count: 3, nonexistent: NONE }]"

[[test.results]]
error = "Incorrect arguments for function math::sum(). Argument 1 was the wrong type. Expected `number` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function math::mean(). Argument 1 was the wrong type. Expected `number` but found `NONE`"

*/

-- Setup data
{
	CREATE data:1 SET a = 10, b = 20;
	CREATE data:2 SET a = 15, b = 25;
	CREATE data:3 SET a = 10, b = 30;
	RETURN "OK";
};

-- Test grouping by nonexistent field (should create NONE groups)
SELECT nonexistent, COUNT() as count FROM data GROUP BY nonexistent;

-- Test aggregate over nonexistent field
SELECT a, math::sum(nonexistent) as total FROM data GROUP BY a;

-- Test grouping and aggregating both on nonexistent fields
SELECT missing_group, math::mean(missing_value) as avg FROM data GROUP BY missing_group;
