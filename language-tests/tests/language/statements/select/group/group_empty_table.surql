/**
[test]
reason = "Test GROUP BY and GROUP ALL on empty tables"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0, total: 0 }]"

[[test.results]]
value = "[]"

*/

-- Setup data
{
	DEFINE TABLE empty_test SCHEMAFULL;
	DEFINE FIELD category ON TABLE empty_test TYPE string;
	DEFINE FIELD value ON TABLE empty_test TYPE int;
	RETURN "OK";
};

-- Test GROUP BY on empty table
SELECT category, COUNT() as count FROM empty_test GROUP BY category;

-- Test GROUP ALL on empty table
SELECT COUNT() as count, math::sum(value) as total FROM empty_test GROUP ALL;

-- Test aggregate functions on empty table with GROUP BY
SELECT category, math::mean(value) as avg, math::min(value) as min, math::max(value) as max FROM empty_test GROUP BY category;
