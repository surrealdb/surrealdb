/**
[test]
reason = "Test ORDER BY with geometry multiline values"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: { coordinates: [[[0f, 0f], [1f, 1f]]], type: 'MultiLineString' } }, { id: item:2, value: { coordinates: [[[0f, 0f], [1f, 1f]], [[2f, 2f], [3f, 3f]]], type: 'MultiLineString' } }, { id: item:3, value: { coordinates: [[[1f, 1f], [2f, 2f]], [[3f, 3f], [4f, 4f]]], type: 'MultiLineString' } }]"

[[test.results]]
value = "[{ id: item:3, value: { coordinates: [[[1f, 1f], [2f, 2f]], [[3f, 3f], [4f, 4f]]], type: 'MultiLineString' } }, { id: item:2, value: { coordinates: [[[0f, 0f], [1f, 1f]], [[2f, 2f], [3f, 3f]]], type: 'MultiLineString' } }, { id: item:1, value: { coordinates: [[[0f, 0f], [1f, 1f]]], type: 'MultiLineString' } }]"
*/

-- Setup: Create records with geometry multiline values
{
	CREATE item:1 SET value = { type: "MultiLineString", coordinates: [[[0.0, 0.0], [1.0, 1.0]]] };
	CREATE item:2 SET value = { type: "MultiLineString", coordinates: [[[0.0, 0.0], [1.0, 1.0]], [[2.0, 2.0], [3.0, 3.0]]] };
	CREATE item:3 SET value = { type: "MultiLineString", coordinates: [[[1.0, 1.0], [2.0, 2.0]], [[3.0, 3.0], [4.0, 4.0]]] };
	RETURN "OK";
};

-- Test ASC: geometry multiline ordering
SELECT * FROM item ORDER BY value ASC;

-- Test DESC
SELECT * FROM item ORDER BY value DESC;
