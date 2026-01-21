/**
[test]
reason = "Test ORDER BY with array values (element-wise comparison)"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: [] }, { id: item:2, value: [1] }, { id: item:3, value: [1, 2] }, { id: item:4, value: [2, 1] }]"

[[test.results]]
value = "[{ id: item:4, value: [2, 1] }, { id: item:3, value: [1, 2] }, { id: item:2, value: [1] }, { id: item:1, value: [] }]"
*/

-- Setup: Create records with array values
{
	CREATE item:1 SET value = [];
	CREATE item:2 SET value = [1];
	CREATE item:3 SET value = [1, 2];
	CREATE item:4 SET value = [2, 1];
	RETURN "OK";
};

-- Test ASC: element-wise comparison, then by length
SELECT * FROM item ORDER BY value ASC;

-- Test DESC
SELECT * FROM item ORDER BY value DESC;
