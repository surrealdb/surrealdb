/**
[test]
reason = "Test ORDER BY with geometry values"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:4, value: (0f, 0f) }, { id: item:2, value: (0f, 1f) }, { id: item:3, value: (1f, 0f) }, { id: item:1, value: (1f, 1f) }]"

[[test.results]]
value = "[{ id: item:1, value: (1f, 1f) }, { id: item:3, value: (1f, 0f) }, { id: item:2, value: (0f, 1f) }, { id: item:4, value: (0f, 0f) }]"

*/

-- Setup: Create records with geometry point values
{
	CREATE item:4 SET value = { type: "Point", coordinates: [0.0, 0.0] };
	CREATE item:2 SET value = { type: "Point", coordinates: [0.0, 1.0] };
	CREATE item:3 SET value = { type: "Point", coordinates: [1.0, 0.0] };
	CREATE item:1 SET value = { type: "Point", coordinates: [1.0, 1.0] };
	RETURN "OK";
};

-- Test ASC: geometry ordering
SELECT * FROM item ORDER BY value ASC;

-- Test DESC
SELECT * FROM item ORDER BY value DESC;
