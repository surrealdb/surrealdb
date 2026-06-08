/**
[env]
imports = ["upgrade/value/geometry_import.surql"]

[test]
upgrade = true
reason = "All seven GeoJSON-shape kinds (Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, and the implicit tuple-point) round-trip through upgrade"

[[test.results]]
value = "[(-0.118092f, 51.509865f)]"

[[test.results]]
value = "[{ type: 'LineString', coordinates: [[0f, 0f], [1f, 1f], [2f, 0f]] }]"

[[test.results]]
value = "[{ type: 'Polygon', coordinates: [[[-1f, -1f], [1f, -1f], [1f, 1f], [-1f, 1f], [-1f, -1f]]] }]"

[[test.results]]
value = "[{ type: 'MultiPoint', coordinates: [[0f, 0f], [1f, 1f], [2f, 2f]] }]"

[[test.results]]
value = "[{ type: 'MultiLineString', coordinates: [[[0f, 0f], [1f, 1f]], [[2f, 2f], [3f, 3f]]] }]"

[[test.results]]
value = "[{ type: 'MultiPolygon', coordinates: [[[[-1f, -1f], [0f, -1f], [0f, 0f], [-1f, 0f], [-1f, -1f]]], [[[1f, 1f], [2f, 1f], [2f, 2f], [1f, 2f], [1f, 1f]]]] }]"

*/
SELECT VALUE shape FROM geo:point;
SELECT VALUE shape FROM geo:line;
SELECT VALUE shape FROM geo:polygon;
SELECT VALUE shape FROM geo:multipoint;
SELECT VALUE shape FROM geo:multiline;
SELECT VALUE shape FROM geo:multipolygon;
