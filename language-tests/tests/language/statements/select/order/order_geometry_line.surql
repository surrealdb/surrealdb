/**
[test]
reason = "Test ORDER BY with geometry line values"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: { coordinates: [[0f, 0f], [1f, 1f]], type: 'LineString' } }, { id: item:2, value: { coordinates: [[0f, 0f], [2f, 2f]], type: 'LineString' } }, { id: item:3, value: { coordinates: [[1f, 1f], [2f, 2f]], type: 'LineString' } }, { id: item:4, value: { coordinates: [[5f, 5f], [10f, 10f]], type: 'LineString' } }]"

[[test.results]]
value = "[{ id: item:4, value: { coordinates: [[5f, 5f], [10f, 10f]], type: 'LineString' } }, { id: item:3, value: { coordinates: [[1f, 1f], [2f, 2f]], type: 'LineString' } }, { id: item:2, value: { coordinates: [[0f, 0f], [2f, 2f]], type: 'LineString' } }, { id: item:1, value: { coordinates: [[0f, 0f], [1f, 1f]], type: 'LineString' } }]"
*/

-- Setup: Create records with geometry line values
{
	CREATE item:1 SET value = { type: "LineString", coordinates: [[0.0, 0.0], [1.0, 1.0]] };
	CREATE item:2 SET value = { type: "LineString", coordinates: [[0.0, 0.0], [2.0, 2.0]] };
	CREATE item:3 SET value = { type: "LineString", coordinates: [[1.0, 1.0], [2.0, 2.0]] };
	CREATE item:4 SET value = { type: "LineString", coordinates: [[5.0, 5.0], [10.0, 10.0]] };
	RETURN "OK";
};

-- Test ASC: geometry line ordering
SELECT * FROM item ORDER BY value ASC;

-- Test DESC
SELECT * FROM item ORDER BY value DESC;
