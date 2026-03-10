/**
[test]
reason = "Test ORDER BY with duration values"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: 1s }, { id: item:2, value: 1m }, { id: item:3, value: 1h }, { id: item:4, value: 1d }]"

[[test.results]]
value = "[{ id: item:4, value: 1d }, { id: item:3, value: 1h }, { id: item:2, value: 1m }, { id: item:1, value: 1s }]"
*/

-- Setup: Create records with duration values
{
	CREATE item:1 SET value = 1s;
	CREATE item:2 SET value = 1m;
	CREATE item:3 SET value = 1h;
	CREATE item:4 SET value = 1d;
	RETURN "OK";
};

-- Test ASC: shortest to longest
SELECT * FROM item ORDER BY value ASC;

-- Test DESC: longest to shortest
SELECT * FROM item ORDER BY value DESC;
