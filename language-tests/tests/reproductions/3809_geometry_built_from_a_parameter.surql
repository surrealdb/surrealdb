/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A GeoJSON object whose coordinates come from a parameter stays an object, as an
object built from computed parts does, but it is the same geometry as the
literal wherever a geometry is expected: a cast, which closes its ring, and
geo::centroid. A collection's members may be GeoJSON objects too; an object of
another geometry kind, or with keys besides its type and coordinates, is not
the geometry asked for.
"""
issue = 3809

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ type: 'Polygon', coordinates: [[[-0.039f, 51.881f], [30.481f, 50.683f], [25.555f, 26.555f], [-0.039f, 51.881f]]] }"

[[test.results]]
value = "[true, false]"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "(18.665666666666667f, 43.03966666666666f)"

[[test.results]]
value = "(18.665666666666667f, 43.03966666666666f)"

[[test.results]]
value = "[]"

[[test.results]]
value = "[(43.44473606666667f, -5.781303666666666f)]"

[[test.results]]
value = "{ type: 'GeometryCollection', geometries: [(-0.039f, 51.881f), { type: 'Polygon', coordinates: [[[-0.039f, 51.881f], [30.481f, 50.683f], [25.555f, 26.555f], [-0.039f, 51.881f]]] }] }"

[[test.results]]
error = "Could not cast into `geometry` using input `{ coordinates: [[[-0.039f, 51.881f], [30.481f, 50.683f], [25.555f, 26.555f]]], type: 'Polygon' }`"

[[test.results]]
error = "Could not cast into `geometry` using input `{ bbox: [0, 0, 1, 1], coordinates: [[[-0.039f, 51.881f], [30.481f, 50.683f], [25.555f, 26.555f]]], type: 'Polygon' }`"
*/
LET $polygon = {
    type: "Polygon",
    coordinates: [[
        [-0.039, 51.881],
        [30.481, 50.683],
        [25.555, 26.555]
    ]]
};
LET $london = [-0.039, 51.881];
LET $param_polygon = {
    type: "Polygon",
    coordinates: [[
        $london,
        [30.481, 50.683],
        [25.555, 26.555]
    ]]
};
RETURN $polygon;
RETURN [type::is_object($param_polygon), type::is_geometry($param_polygon)];
RETURN $polygon == $param_polygon;
RETURN $polygon == <geometry> $param_polygon;
RETURN type::is_geometry(<geometry<polygon>> $param_polygon);
RETURN geo::centroid($polygon);
RETURN geo::centroid($param_polygon);
UPSERT way:test SET geometry = { type: "MultiPoint", coordinates: [[43.4445928f, -5.7814557f], [43.4447708f, -5.7812748f], [43.4448446f, -5.7811805f]] } RETURN NONE;
SELECT VALUE geo::centroid({ type: "MultiPoint", coordinates: geometry.coordinates }) FROM way;
RETURN <geometry> { type: "GeometryCollection", geometries: [{ type: "Point", coordinates: $london }, $param_polygon] };
RETURN <geometry<point>> $param_polygon;
RETURN <geometry> { type: "Polygon", coordinates: $param_polygon.coordinates, bbox: [0, 0, 1, 1] };
