/**
[env]
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

-- GROUP ALL on an empty table: the one group is the statement's, not the
-- data's, so it reports the grand total of no rows — each aggregate's
-- answer over no values. GROUP BY above and below has no such group,
-- because a key can only come from a row.
SELECT COUNT() as count, math::sum(value) as total FROM empty_test GROUP ALL;

-- Test aggregate functions on empty table with GROUP BY
SELECT category, math::mean(value) as avg, math::min(value) as min, math::max(value) as max FROM empty_test GROUP BY category;
