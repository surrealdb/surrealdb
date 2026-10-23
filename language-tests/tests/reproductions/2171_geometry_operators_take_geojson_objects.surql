/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A GeoJSON object that arrives as data, such as a query parameter sent by a
client, is a geometry to INSIDE, CONTAINS, INTERSECTS and OUTSIDE, as it already
is to a cast, a typed field and a geo:: function. An object's keys still answer
a string, and an object that is not exactly GeoJSON is still not a geometry.
"""
issue = 2171

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[true, true, true]"

[[test.results]]
value = "{ id: zone:sq }"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "[zone:sq]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"
*/
LET $p = encoding::json::decode('{"type":"Point","coordinates":[0.5,0.5]}');
LET $far = encoding::json::decode('{"type":"Point","coordinates":[5,5]}');
LET $a = encoding::json::decode('{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}');
LET $l = encoding::json::decode('{"type":"LineString","coordinates":[[-1,0.5],[2,0.5]]}');
-- They are objects, not geometries
[type::is_object($p), type::is_object($a), type::is_object($l)];
CREATE ONLY zone:sq SET area = <geometry> $a RETURN id;
-- Against a stored geometry
$p INSIDE zone:sq.area;
zone:sq.area CONTAINS $p;
$l INTERSECTS zone:sq.area;
$far OUTSIDE zone:sq.area;
$far INSIDE zone:sq.area;
SELECT VALUE id FROM zone WHERE $p INSIDE area;
-- Both operands GeoJSON objects
$p INSIDE $a;
$a CONTAINS $p;
$l INTERSECTS $a;
$a CONTAINSALL [$p, (0.25, 0.25)];
-- An object's keys still answer a string
$a CONTAINS 'coordinates';
-- An object with another key is not a geometry
{ type: 'Point', coordinates: [0.5, 0.5], name: 'centre' } INSIDE zone:sq.area;
