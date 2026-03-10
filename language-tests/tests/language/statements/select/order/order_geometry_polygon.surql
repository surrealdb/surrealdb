/**
[test]
reason = "Test ORDER BY with geometry polygon values"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: { coordinates: [[[0f, 0f], [0f, 1f], [1f, 1f], [1f, 0f], [0f, 0f]]], type: 'Polygon' } }, { id: item:2, value: { coordinates: [[[0f, 0f], [0f, 2f], [2f, 2f], [2f, 0f], [0f, 0f]]], type: 'Polygon' } }, { id: item:3, value: { coordinates: [[[1f, 1f], [1f, 3f], [3f, 3f], [3f, 1f], [1f, 1f]]], type: 'Polygon' } }]"

[[test.results]]
value = "[{ id: item:3, value: { coordinates: [[[1f, 1f], [1f, 3f], [3f, 3f], [3f, 1f], [1f, 1f]]], type: 'Polygon' } }, { id: item:2, value: { coordinates: [[[0f, 0f], [0f, 2f], [2f, 2f], [2f, 0f], [0f, 0f]]], type: 'Polygon' } }, { id: item:1, value: { coordinates: [[[0f, 0f], [0f, 1f], [1f, 1f], [1f, 0f], [0f, 0f]]], type: 'Polygon' } }]"
*/

-- Setup: Create records with geometry polygon values
{
	CREATE item:1 SET value = { type: "Polygon", coordinates: [[[0.0, 0.0], [0.0, 1.0], [1.0, 1.0], [1.0, 0.0], [0.0, 0.0]]] };
	CREATE item:2 SET value = { type: "Polygon", coordinates: [[[0.0, 0.0], [0.0, 2.0], [2.0, 2.0], [2.0, 0.0], [0.0, 0.0]]] };
	CREATE item:3 SET value = { type: "Polygon", coordinates: [[[1.0, 1.0], [1.0, 3.0], [3.0, 3.0], [3.0, 1.0], [1.0, 1.0]]] };
	RETURN "OK";
};

-- Test ASC: geometry polygon ordering
SELECT * FROM item ORDER BY value ASC;

-- Test DESC
SELECT * FROM item ORDER BY value DESC;
