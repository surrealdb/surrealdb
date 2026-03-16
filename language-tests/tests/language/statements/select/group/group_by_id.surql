/**
[test]
reason = "Test GROUP BY the id field specifically (should create one group per record)"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: thing:1, value: [10] }, { id: thing:2, value: [20] }, { id: thing:3, value: [10] }, { id: thing:4, value: [30] }]"

[[test.results]]
value = "[{ count: 1, id: thing:1, total: 10f }, { count: 1, id: thing:2, total: 20f }, { count: 1, id: thing:3, total: 10f }, { count: 1, id: thing:4, total: 30f }]"

[[test.results]]
value = "[{ id: thing:1, value: [10] }, { id: thing:2, value: [20] }, { id: thing:3, value: [10] }, { id: thing:4, value: [30] }]"

*/

-- Setup data
{
	CREATE thing:1 SET value = 10;
	CREATE thing:2 SET value = 20;
	CREATE thing:3 SET value = 10;
	CREATE thing:4 SET value = 30;
	RETURN "OK";
};

-- Test grouping by id field (each record should be its own group)
SELECT id, value FROM thing GROUP BY id;

-- Test grouping by id with aggregates
SELECT id, COUNT() as count, math::sum(value) as total FROM thing GROUP BY id;

-- Test grouping by id with ORDER BY
SELECT id, value FROM thing GROUP BY id ORDER BY id;
