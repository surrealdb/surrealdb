/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Geometry::contains on (Line, MultiLine), (Polygon, MultiPolygon), (MultiPoint, MultiPoint), and (MultiLine, MultiLine) previously tested w.iter().all(|x| w.contains(x)) instead of v.contains(x)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"
*/

LET $outer_poly = <geometry> {
	type: "Polygon",
	coordinates: [[[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0], [0.0, 0.0]]]
};
LET $inner_mp = <geometry> {
	type: "MultiPolygon",
	coordinates: [
		[[[1.0, 1.0], [3.0, 1.0], [3.0, 3.0], [1.0, 3.0], [1.0, 1.0]]],
		[[[5.0, 5.0], [7.0, 5.0], [7.0, 7.0], [5.0, 7.0], [5.0, 5.0]]]
	]
};
LET $outside_mp = <geometry> {
	type: "MultiPolygon",
	coordinates: [
		[[[1.0, 1.0], [3.0, 1.0], [3.0, 3.0], [1.0, 3.0], [1.0, 1.0]]],
		[[[20.0, 20.0], [22.0, 20.0], [22.0, 22.0], [20.0, 22.0], [20.0, 20.0]]]
	]
};
LET $ml = <geometry> {
	type: "MultiLineString",
	coordinates: [
		[[1.0, 1.0], [3.0, 3.0]],
		[[5.0, 5.0], [7.0, 7.0]]
	]
};
LET $mpt = <geometry> {
	type: "MultiPoint",
	coordinates: [[2.0, 2.0], [6.0, 6.0]]
};
LET $col_in = <geometry> {
	type: "GeometryCollection",
	geometries: [
		{ type: "Point", coordinates: [2.0, 2.0] },
		{ type: "LineString", coordinates: [[5.0, 5.0], [6.0, 6.0]] }
	]
};
LET $col_out = <geometry> {
	type: "GeometryCollection",
	geometries: [
		{ type: "Point", coordinates: [2.0, 2.0] },
		{ type: "Point", coordinates: [25.0, 25.0] }
	]
};

-- Polygon CONTAINS MultiPolygon / MultiPolygon INSIDE Polygon
$outer_poly CONTAINS $inner_mp;
$inner_mp INSIDE $outer_poly;
$outer_poly CONTAINS $outside_mp;
$outside_mp INSIDE $outer_poly;

-- MultiPolygon CONTAINS Point / Polygon
$inner_mp CONTAINS (2.0, 2.0);
$inner_mp CONTAINS (4.0, 4.0);

-- MultiLineString CONTAINS Point / LineString
$ml CONTAINS (2.0, 2.0);
$ml CONTAINS (4.0, 4.0);

-- MultiPoint CONTAINS Point / MultiPoint
$mpt CONTAINS (2.0, 2.0);
$mpt CONTAINS (4.0, 4.0);

-- GeometryCollection CONTAINS / INSIDE
$outer_poly CONTAINS $col_in;
$col_in INSIDE $outer_poly;
$outer_poly CONTAINS $col_out;

-- Empty MultiPolygon and GeometryCollection are vacuously contained
$outer_poly CONTAINS <geometry> { type: "MultiPolygon", coordinates: [] };
$outer_poly CONTAINS <geometry> { type: "GeometryCollection", geometries: [] };

-- Pinned unchanged receiver combinations (`Polygon CONTAINS MultiPoint` and `GeometryCollection CONTAINS Point`)
RETURN { type: "Polygon", coordinates: [[[0.0, 0.0], [2.0, 0.0], [2.0, 2.0], [0.0, 2.0], [0.0, 0.0]]] }
	CONTAINS { type: "MultiPoint", coordinates: [[1.0, 1.0], [1.5, 1.5]] };
RETURN {
	type: "GeometryCollection",
	geometries: [
		{ type: "Polygon", coordinates: [[[0.0, 0.0], [2.0, 0.0], [2.0, 2.0], [0.0, 2.0], [0.0, 0.0]]] },
		{ type: "Point", coordinates: [50.0, 50.0] }
	]
} CONTAINS (1.0, 1.0);
