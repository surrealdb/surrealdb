/**
[test]
reason = "Test ORDER BY with mixed geometry types"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:5, value: (0f, 0f) }, { id: item:2, value: { type: 'LineString', coordinates: [[0f, 0f], [1f, 1f]] } }, { id: item:6, value: { type: 'Polygon', coordinates: [[[0f, 0f], [0f, 1f], [1f, 1f], [1f, 0f], [0f, 0f]]] } }, { id: item:3, value: { type: 'MultiPoint', coordinates: [[0f, 0f], [1f, 1f]] } }, { id: item:4, value: { type: 'MultiLineString', coordinates: [[[0f, 0f], [1f, 1f]]] } }, { id: item:7, value: { type: 'MultiPolygon', coordinates: [[[[0f, 0f], [0f, 1f], [1f, 1f], [1f, 0f], [0f, 0f]]]] } }, { id: item:1, value: { type: 'GeometryCollection', geometries: [(0f, 0f)] } }]"

[[test.results]]
value = "[{ id: item:1, value: { type: 'GeometryCollection', geometries: [(0f, 0f)] } }, { id: item:7, value: { type: 'MultiPolygon', coordinates: [[[[0f, 0f], [0f, 1f], [1f, 1f], [1f, 0f], [0f, 0f]]]] } }, { id: item:4, value: { type: 'MultiLineString', coordinates: [[[0f, 0f], [1f, 1f]]] } }, { id: item:3, value: { type: 'MultiPoint', coordinates: [[0f, 0f], [1f, 1f]] } }, { id: item:6, value: { type: 'Polygon', coordinates: [[[0f, 0f], [0f, 1f], [1f, 1f], [1f, 0f], [0f, 0f]]] } }, { id: item:2, value: { type: 'LineString', coordinates: [[0f, 0f], [1f, 1f]] } }, { id: item:5, value: (0f, 0f) }]"
*/

-- Setup: Create records with different geometry types
{
	CREATE item:1 SET value = { type: "GeometryCollection", geometries: [{ type: "Point", coordinates: [0.0, 0.0] }] };
	CREATE item:2 SET value = { type: "LineString", coordinates: [[0.0, 0.0], [1.0, 1.0]] };
	CREATE item:3 SET value = { type: "MultiPoint", coordinates: [[0.0, 0.0], [1.0, 1.0]] };
	CREATE item:4 SET value = { type: "MultiLineString", coordinates: [[[0.0, 0.0], [1.0, 1.0]]] };
	CREATE item:5 SET value = { type: "Point", coordinates: [0.0, 0.0] };
	CREATE item:6 SET value = { type: "Polygon", coordinates: [[[0.0, 0.0], [0.0, 1.0], [1.0, 1.0], [1.0, 0.0], [0.0, 0.0]]] };
	CREATE item:7 SET value = { type: "MultiPolygon", coordinates: [[[[0.0, 0.0], [0.0, 1.0], [1.0, 1.0], [1.0, 0.0], [0.0, 0.0]]]] };
	RETURN "OK";
};

-- Test ASC: ordering across different geometry types
SELECT * FROM item ORDER BY value ASC;

-- Test DESC
SELECT * FROM item ORDER BY value DESC;
