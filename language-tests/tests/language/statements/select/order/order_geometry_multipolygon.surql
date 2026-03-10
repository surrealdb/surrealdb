/**
[test]
reason = "Test ORDER BY with geometry multipolygon values"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: { coordinates: [[[[0f, 0f], [0f, 1f], [1f, 1f], [1f, 0f], [0f, 0f]]]], type: 'MultiPolygon' } }, { id: item:2, value: { coordinates: [[[[0f, 0f], [0f, 2f], [2f, 2f], [2f, 0f], [0f, 0f]]]], type: 'MultiPolygon' } }, { id: item:3, value: { coordinates: [[[[1f, 1f], [1f, 2f], [2f, 2f], [2f, 1f], [1f, 1f]]]], type: 'MultiPolygon' } }]"

[[test.results]]
value = "[{ id: item:3, value: { coordinates: [[[[1f, 1f], [1f, 2f], [2f, 2f], [2f, 1f], [1f, 1f]]]], type: 'MultiPolygon' } }, { id: item:2, value: { coordinates: [[[[0f, 0f], [0f, 2f], [2f, 2f], [2f, 0f], [0f, 0f]]]], type: 'MultiPolygon' } }, { id: item:1, value: { coordinates: [[[[0f, 0f], [0f, 1f], [1f, 1f], [1f, 0f], [0f, 0f]]]], type: 'MultiPolygon' } }]"
*/

-- Setup: Create records with geometry multipolygon values
{
	CREATE item:1 SET value = { type: "MultiPolygon", coordinates: [[[[0.0, 0.0], [0.0, 1.0], [1.0, 1.0], [1.0, 0.0], [0.0, 0.0]]]] };
	CREATE item:2 SET value = { type: "MultiPolygon", coordinates: [[[[0.0, 0.0], [0.0, 2.0], [2.0, 2.0], [2.0, 0.0], [0.0, 0.0]]]] };
	CREATE item:3 SET value = { type: "MultiPolygon", coordinates: [[[[1.0, 1.0], [1.0, 2.0], [2.0, 2.0], [2.0, 1.0], [1.0, 1.0]]]] };
	RETURN "OK";
};

-- Test ASC: geometry multipolygon ordering
SELECT * FROM item ORDER BY value ASC;

-- Test DESC
SELECT * FROM item ORDER BY value DESC;
