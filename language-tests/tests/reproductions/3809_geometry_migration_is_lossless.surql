/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A migration that keeps `$value` and retypes a field to `geometry` takes a
stored GeoJSON object as its geometry only when nothing of it changes: a closed
ring of floats converts, while an open ring, which the geometry closes with an
added point, and a decimal coordinate, which it rounds, take the FALLBACK.
"""
issue = 3809

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[m:closed, true, false], [m:decimal, true, true], [m:open, true, true]]"
*/
LET $closed = [[[0f, 0f], [1f, 0f], [1f, 1f], [0f, 0f]]];
LET $open = [[[0f, 0f], [1f, 0f], [1f, 1f]]];
LET $decimal = [0.1000000000000000000000001dec, 2f];
DEFINE TABLE m SCHEMALESS;
CREATE m:closed SET g = { type: 'Polygon', coordinates: $closed } RETURN NONE;
CREATE m:open SET g = { type: 'Polygon', coordinates: $open } RETURN NONE;
CREATE m:decimal SET g = { type: 'Point', coordinates: $decimal } RETURN NONE;
DEFINE FIELD g ON m TYPE geometry MIGRATE EXISTING ROWS AS to_geo USING $value FALLBACK (9, 9);
SELECT VALUE [id, type::is_geometry(g), g == (9, 9)] FROM m ORDER BY id;
