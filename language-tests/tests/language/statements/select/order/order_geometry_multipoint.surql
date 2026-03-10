/**
[test]
reason = "Test ORDER BY with geometry multipoint values"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: { coordinates: [[0f, 0f], [1f, 1f]], type: 'MultiPoint' } }, { id: item:2, value: { coordinates: [[0f, 0f], [2f, 2f]], type: 'MultiPoint' } }, { id: item:3, value: { coordinates: [[1f, 1f], [2f, 2f], [3f, 3f]], type: 'MultiPoint' } }]"

[[test.results]]
value = "[{ id: item:3, value: { coordinates: [[1f, 1f], [2f, 2f], [3f, 3f]], type: 'MultiPoint' } }, { id: item:2, value: { coordinates: [[0f, 0f], [2f, 2f]], type: 'MultiPoint' } }, { id: item:1, value: { coordinates: [[0f, 0f], [1f, 1f]], type: 'MultiPoint' } }]"
*/

-- Setup: Create records with geometry multipoint values
{
	CREATE item:1 SET value = { type: "MultiPoint", coordinates: [[0.0, 0.0], [1.0, 1.0]] };
	CREATE item:2 SET value = { type: "MultiPoint", coordinates: [[0.0, 0.0], [2.0, 2.0]] };
	CREATE item:3 SET value = { type: "MultiPoint", coordinates: [[1.0, 1.0], [2.0, 2.0], [3.0, 3.0]] };
	RETURN "OK";
};

-- Test ASC: geometry multipoint ordering
SELECT * FROM item ORDER BY value ASC;

-- Test DESC
SELECT * FROM item ORDER BY value DESC;
