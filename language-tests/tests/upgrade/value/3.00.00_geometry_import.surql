/**
[test]

[[test.results]]
value = "[{ id: geo:point, shape: (-0.118092f, 51.509865f) }]"

[[test.results]]
value = "[{ id: geo:line, shape: { type: 'LineString', coordinates: [[0f, 0f], [1f, 1f], [2f, 0f]] } }]"

[[test.results]]
value = "[{ id: geo:polygon, shape: { type: 'Polygon', coordinates: [[[-1f, -1f], [1f, -1f], [1f, 1f], [-1f, 1f], [-1f, -1f]]] } }]"

[[test.results]]
value = "[{ id: geo:multipoint, shape: { type: 'MultiPoint', coordinates: [[0f, 0f], [1f, 1f], [2f, 2f]] } }]"

[[test.results]]
value = "[{ id: geo:multiline, shape: { type: 'MultiLineString', coordinates: [[[0f, 0f], [1f, 1f]], [[2f, 2f], [3f, 3f]]] } }]"

[[test.results]]
value = "[{ id: geo:multipolygon, shape: { type: 'MultiPolygon', coordinates: [[[[-1f, -1f], [0f, -1f], [0f, 0f], [-1f, 0f], [-1f, -1f]]], [[[1f, 1f], [2f, 1f], [2f, 2f], [1f, 2f], [1f, 1f]]]] } }]"

*/
CREATE geo:point SET shape = (-0.118092, 51.509865);
CREATE geo:line SET shape = { type: 'LineString', coordinates: [[0,0],[1,1],[2,0]] };
CREATE geo:polygon SET shape = { type: 'Polygon', coordinates: [[[-1,-1],[1,-1],[1,1],[-1,1],[-1,-1]]] };
CREATE geo:multipoint SET shape = { type: 'MultiPoint', coordinates: [[0,0],[1,1],[2,2]] };
CREATE geo:multiline SET shape = { type: 'MultiLineString', coordinates: [[[0,0],[1,1]],[[2,2],[3,3]]] };
CREATE geo:multipolygon SET shape = { type: 'MultiPolygon', coordinates: [[[[-1,-1],[0,-1],[0,0],[-1,0],[-1,-1]]],[[[1,1],[2,1],[2,2],[1,2],[1,1]]]] };
