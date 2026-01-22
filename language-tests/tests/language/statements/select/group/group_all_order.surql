/**
[test]
reason = "Test GROUP ALL with ORDER BY - verify single result ordering behavior"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ count: 3, total: 45f }]"

[[test.results]]
value = "[{ avg: 15f }]"

*/

-- Setup data
{
	CREATE item:1 SET value = 10;
	CREATE item:2 SET value = 20;
	CREATE item:3 SET value = 15;
	RETURN "OK";
};

-- Test ORDER BY with GROUP ALL (single result, ordering should not affect output)
SELECT math::sum(value) as total, COUNT() as count FROM item GROUP ALL ORDER BY total;

-- Test ORDER BY DESC with GROUP ALL
SELECT math::mean(value) as avg FROM item GROUP ALL ORDER BY avg DESC;
