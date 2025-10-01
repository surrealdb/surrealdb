/**
[test]

[[test.results]]
value = "{ id: location:[50f, 50f] }"

[[test.results]]
value = "{ id: location:[50f, 50f], point: (50, 50) }"

[[test.results]]
value = "131780f"

*/

// Make a location with two floats
CREATE ONLY location:[50.0, 50.0];
// Cast into a point to set a field
UPDATE ONLY location:[50.0, 50.0] SET point = <point>[id[0], id[1]];
// Use it as a geo type
geo::distance(location:[50.0, 50.0].point, (51.0, 51.0)).floor();