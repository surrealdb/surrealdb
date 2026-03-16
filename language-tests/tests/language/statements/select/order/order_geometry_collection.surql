/**
[test]
reason = "Test ORDER BY with geometry collection values"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:1, value: { geometries: [{ coordinates: [0f, 0f], type: 'Point' }], type: 'GeometryCollection' } }, { id: item:2, value: { geometries: [{ coordinates: [0f, 0f], type: 'Point' }, { coordinates: [[1f, 1f], [2f, 2f]], type: 'LineString' }], type: 'GeometryCollection' } }, { id: item:3, value: { geometries: [{ coordinates: [1f, 1f], type: 'Point' }], type: 'GeometryCollection' } }]"

[[test.results]]
value = "[{ id: item:3, value: { geometries: [{ coordinates: [1f, 1f], type: 'Point' }], type: 'GeometryCollection' } }, { id: item:2, value: { geometries: [{ coordinates: [0f, 0f], type: 'Point' }, { coordinates: [[1f, 1f], [2f, 2f]], type: 'LineString' }], type: 'GeometryCollection' } }, { id: item:1, value: { geometries: [{ coordinates: [0f, 0f], type: 'Point' }], type: 'GeometryCollection' } }]"
*/

-- Setup: Create records with geometry collection values
{
	CREATE item:1 SET value = { type: "GeometryCollection", geometries: [{ type: "Point", coordinates: [0.0, 0.0] }] };
	CREATE item:2 SET value = { type: "GeometryCollection", geometries: [{ type: "Point", coordinates: [0.0, 0.0] }, { type: "LineString", coordinates: [[1.0, 1.0], [2.0, 2.0]] }] };
	CREATE item:3 SET value = { type: "GeometryCollection", geometries: [{ type: "Point", coordinates: [1.0, 1.0] }] };
	RETURN "OK";
};

-- Test ASC: geometry collection ordering
SELECT * FROM item ORDER BY value ASC;

-- Test DESC
SELECT * FROM item ORDER BY value DESC;
