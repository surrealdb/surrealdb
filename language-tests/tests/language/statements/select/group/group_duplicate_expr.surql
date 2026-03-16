/**
[test]
reason = "Test GROUP BY with duplicate group expressions"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ count: 2, field: 'A' }, { count: 2, field: 'B' }]"

[[test.results]]
value = "[{ count: 2, field: 'A' }, { count: 2, field: 'B' }]"
*/

-- Setup data
{
	CREATE dup:1 SET field = 'A', value = 10;
	CREATE dup:2 SET field = 'B', value = 20;
	CREATE dup:3 SET field = 'A', value = 15;
	CREATE dup:4 SET field = 'B', value = 25;
	RETURN "OK";
};

-- Baseline
SELECT field, COUNT() as count FROM dup GROUP BY field;

-- Test same field with function expressions
SELECT field, COUNT() as count FROM dup GROUP BY field, field;
