/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A GeoJSON object becomes a geometry only where nothing else holds it. A union
that can hold it as an object keeps it one, whichever order the union names
its members in: on a write, on a later write of a stored row after the field
is retyped, through a cast and as a function argument. A field typed
`geometry` alone converts it, and a migration converts stored rows when it
says so.
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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[true, true, true]]"

[[test.results]]
value = "[true, true]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[true, true]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[true, true]]"
*/
LET $c = [1, 2];
LET $p = { type: 'Point', coordinates: $c };
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD a ON t TYPE option<geometry | object>;
DEFINE FIELD b ON t TYPE option<object | geometry>;
DEFINE FIELD g ON t TYPE option<geometry>;
CREATE t:1 SET a = $p, b = $p, g = $p RETURN VALUE [type::is_object(a), type::is_object(b), type::is_geometry(g)];
RETURN [type::is_object(<geometry | object> $p), type::is_geometry(<geometry> $p)];
DEFINE FUNCTION fn::kind($v: geometry | object) { RETURN type::is_object($v) };
RETURN fn::kind($p);
DEFINE TABLE s SCHEMALESS;
DEFINE FIELD obj ON s TYPE object;
DEFINE FIELD geo ON s TYPE geometry;
CREATE s:1 SET obj = $p, geo = $p RETURN NONE;
DEFINE FIELD OVERWRITE obj ON s TYPE geometry | object;
DEFINE FIELD OVERWRITE geo ON s TYPE object | geometry;
UPDATE s:1 SET n = 1 RETURN VALUE [type::is_object(obj), type::is_geometry(geo)];
ALTER FIELD obj ON s TYPE geometry | object MIGRATE EXISTING ROWS AS to_geo USING <geometry> $value FALLBACK {};
SELECT VALUE [type::is_geometry(obj), type::is_geometry(geo)] FROM s:1;
