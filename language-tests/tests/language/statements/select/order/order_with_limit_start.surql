/**
[test]
reason = "Test ORDER BY combined with LIMIT and START clauses"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: 10 }, { id: item:2, value: 20 }, { id: item:3, value: 30 }]"

[[test.results]]
value = "[{ id: item:2, value: 20 }, { id: item:3, value: 30 }, { id: item:4, value: 40 }]"

[[test.results]]
value = "[{ id: item:4, value: 40 }, { id: item:3, value: 30 }]"

[[test.results]]
value = "[{ id: item:3, value: 30 }, { id: item:4, value: 40 }]"
*/

-- Setup: Create records with values
{
	CREATE item:1 SET value = 10;
	CREATE item:2 SET value = 20;
	CREATE item:3 SET value = 30;
	CREATE item:4 SET value = 40;
	CREATE item:5 SET value = 50;
	RETURN "OK";
};

-- Test: ORDER BY with LIMIT
SELECT * FROM item ORDER BY value ASC LIMIT 3;

-- Test: ORDER BY with START
SELECT * FROM item ORDER BY value ASC START 1 LIMIT 3;

-- Test: ORDER BY DESC with START and LIMIT
SELECT * FROM item ORDER BY value DESC START 1 LIMIT 2;

-- Test: ORDER BY ASC with START and LIMIT  
SELECT * FROM item ORDER BY value ASC START 2 LIMIT 2;
