/**
[test]

[[test.results]]
value = "{ id: location:[50f, 50f] }"

[[test.results]]
value = "{ id: location:[50f, 50f], point: (50, 50) }"

[[test.results]]
value = "131780f"

[[test.results]]
value = "(0.0, 0.0)"

[[test.results]]
value = "(0.0, 0.0)"

[[test.results]]
error = "Expected `point` but found a `[0f, 0f, 0f]`"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

*/

// Make a location with two floats
CREATE ONLY location:[50.0, 50.0];
// Cast into a point to set a field
UPDATE ONLY location:[50.0, 50.0] SET point = <point>[id[0], id[1]];
// Use it as a geo type
geo::distance(location:[50.0, 50.0].point, (51.0, 51.0)).floor();

// A point can cast into itself
<point>(0.0, 0.0);

// So can an array of len 2
<point>[0.0, 0.0];

// But not a longer array
<point>[0.0, 0.0, 0.0];

// GeoJSON objects work too
(<point>{ type: "Point", coordinates: [0.0, 0.0] }).is_point();

// Multi-type casting works too
(<geometry<polygon | point>> { type: "Polygon", coordinates: [[[9.9, 9.9], [10.0, 10.0], [11.0, 11.0]]] }).is_geometry();
(<geometry<point|polygon>> { type: "Polygon", coordinates: [[[9.9, 9.9], [10.0, 10.0], [11.0, 11.0]]] }).is_polygon();
(<geometry<point|polygon>> { type: "Polygon", coordinates: [[[9.9, 9.9], [10.0, 10.0], [11.0, 11.0]]] }).is_point();