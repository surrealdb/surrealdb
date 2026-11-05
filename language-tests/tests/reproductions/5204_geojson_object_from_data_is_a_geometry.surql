/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A GeoJSON object that arrives as data (decoded JSON, a field read, an object
built by a closure) is accepted as a geometry wherever one is expected.
"""
issue = 5204

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[true]"
*/
LET $doc = '{"features":[{"properties":{"ISO":"MC"},"geometry":{"type":"Polygon","coordinates":[[[7.4392912127426,43.7575180718222],[7.42028222975404,43.7370821536331],[7.39160908001543,43.7275450565354],[7.39090013864279,43.7493732088458],[7.42899119481586,43.7683000491831],[7.4392912127426,43.7575180718222]]]}}]}';
LET $test = encoding::json::decode($doc).features.map(|$row| {
    iso: $row.properties.ISO,
    geometry: $row.geometry,
});
RETURN type::is_geometry($test[0].geometry);
RETURN geo::is_valid($test[0].geometry);
DEFINE TABLE assets SCHEMAFULL;
DEFINE FIELD geoschema ON assets TYPE geometry<polygon>;
CREATE assets:1 SET geoschema = $test[0].geometry RETURN VALUE type::is_geometry(geoschema);
