/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
Once an object is recognised as a GeometryCollection, every member is a
geometry value: reading a member, comparing it, indexing it and storing it all
see a Point, not a raw coordinate array, whether the member was written as a
tuple or as a GeoJSON object, and at any nesting depth.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "(1f, 2f)"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[true, true]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "(5f, 6f)"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "(1f, 2f)"

[[test.results]]
value = "(2f, 3f)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ g: { geometries: [(1f, 2f), { coordinates: [[0f, 0f], [1f, 1f]], type: 'LineString' }], type: 'GeometryCollection' }, id: gc:1 }]"

[[test.results]]
value = "[true]"

[[test.results]]
value = "[gc:1]"

[[test.results]]
value = "[gc:1]"

[[test.results]]
value = "[{ coordinates: [[0f, 0f], [1f, 1f]], type: 'LineString' }]"
*/
LET $g = { type: "GeometryCollection", geometries: [(1,2)] };
RETURN $g.geometries[0];
RETURN type::is_geometry($g.geometries[0]);
RETURN $g.geometries[0] = (1,2);
RETURN $g.geometries[0] = { type: "Point", coordinates: [1,2] };
LET $o = { type: "GeometryCollection", geometries: [{ type: "Point", coordinates: [1,2] }, (3,4)] };
RETURN [type::is_geometry($o.geometries[0]), type::is_geometry($o.geometries[1])];
LET $n = { type: "GeometryCollection", geometries: [{ type: "GeometryCollection", geometries: [(5,6)] }] };
RETURN $n.geometries[0].geometries[0];
RETURN type::is_geometry($n.geometries[0].geometries[0]);
RETURN $n.geometries[0].geometries[0] = (5,6);
RETURN geo::centroid($g);
RETURN geo::centroid($o);
DEFINE TABLE gc SCHEMALESS;
DEFINE FIELD g ON gc TYPE geometry<collection>;
CREATE gc:1 SET g = { type: "GeometryCollection", geometries: [(1,2), { type: "LineString", coordinates: [[0,0],[1,1]] }] };
SELECT VALUE type::is_geometry(g.geometries[0]) FROM gc;
SELECT VALUE id FROM gc WHERE g.geometries[0] = (1,2);
SELECT VALUE id FROM gc WHERE g.geometries[0] = { type: "Point", coordinates: [1,2] };
SELECT VALUE g.geometries[1] FROM gc;
