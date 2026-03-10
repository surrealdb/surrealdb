/**
[test]
reason = "Test ORDER BY with datetime values"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: d'2020-01-01T00:00:00Z' }, { id: item:2, value: d'2021-06-15T12:30:00Z' }, { id: item:3, value: d'2023-03-20T08:45:00Z' }, { id: item:4, value: d'2024-12-31T23:59:59Z' }]"

[[test.results]]
value = "[{ id: item:4, value: d'2024-12-31T23:59:59Z' }, { id: item:3, value: d'2023-03-20T08:45:00Z' }, { id: item:2, value: d'2021-06-15T12:30:00Z' }, { id: item:1, value: d'2020-01-01T00:00:00Z' }]"
*/

-- Setup: Create records with datetime values
{
	CREATE item:1 SET value = d"2020-01-01T00:00:00Z";
	CREATE item:2 SET value = d"2021-06-15T12:30:00Z";
	CREATE item:3 SET value = d"2023-03-20T08:45:00Z";
	CREATE item:4 SET value = d"2024-12-31T23:59:59Z";
	RETURN "OK";
};

-- Test ASC: oldest to newest
SELECT * FROM item ORDER BY value ASC;

-- Test DESC: newest to oldest
SELECT * FROM item ORDER BY value DESC;
