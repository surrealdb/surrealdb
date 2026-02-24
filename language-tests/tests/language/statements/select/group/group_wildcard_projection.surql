/**
[test]
reason = "Test GROUP BY with wildcard (*) projection to verify which fields appear"

[[test.results]]
value = "'OK'"

[[test.results]]
error = "Invalid query: Incorrect selector for aggregate selection, expression `*` within in selector cannot be aggregated in a group."

[[test.results]]
error = "Invalid query: Incorrect selector for aggregate selection, expression `*` within in selector cannot be aggregated in a group."

*/

-- Setup data
{
	CREATE wild:1 SET category = 'A', name = 'Item1', value = 10;
	CREATE wild:2 SET category = 'A', name = 'Item2', value = 20;
	CREATE wild:3 SET category = 'B', name = 'Item3', value = 15;
	CREATE wild:4 SET category = 'B', name = 'Item4', value = 25;
	RETURN "OK";
};

-- Test wildcard with GROUP BY
SELECT *, COUNT() as count FROM wild GROUP BY category;

-- Test wildcard with aggregates
SELECT *, math::sum(value) as total FROM wild GROUP BY category;
