/**
[test]
reason = "Test ORDER BY with range values"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: 1..5 }, { id: item:2, value: 1..10 }, { id: item:3, value: 10..20 }, { id: item:4, value: 100..200 }]"

[[test.results]]
value = "[{ id: item:4, value: 100..200 }, { id: item:3, value: 10..20 }, { id: item:2, value: 1..10 }, { id: item:1, value: 1..5 }]"
*/

-- Setup: Create records with range values
{
	CREATE item:1 SET value = 1..5;
	CREATE item:2 SET value = 1..10;
	CREATE item:3 SET value = 10..20;
	CREATE item:4 SET value = 100..200;
	RETURN "OK";
};

-- Test ASC: ranges ordered by start, then end
SELECT * FROM item ORDER BY value ASC;

-- Test DESC
SELECT * FROM item ORDER BY value DESC;
