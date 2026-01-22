/**
[test]
wip = true
reason = "Test GROUP BY with computed expressions - showing current limitations"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ value: 20, count: 2 }, { value: 30, count: 1 }, { value: 40, count: 2 }]"

*/

-- Setup data
{
	CREATE calc:1 SET value = 10, multiplier = 2, name = 'Alice';
	CREATE calc:2 SET value = 20, multiplier = 2, name = 'BOB';
	CREATE calc:3 SET value = 10, multiplier = 3, name = 'charlie';
	CREATE calc:4 SET value = 15, multiplier = 2, name = 'Alice';
	CREATE calc:5 SET value = 20, multiplier = 3, name = 'bob';
	RETURN "OK";
};

-- Test grouping by arithmetic expression (currently not supported)
SELECT `value` * 2 AS value, COUNT() as count FROM calc GROUP BY value * 2 ORDER BY value;
