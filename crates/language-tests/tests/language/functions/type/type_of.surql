/**
[env.capabilities]
allow-experimental = ["files"]

[test]

[[test.results]]
value = "['none', 'none']"

[[test.results]]
value = "['null', 'null']"

[[test.results]]
value = "['bool', 'bool']"

[[test.results]]
value = "['uuid', 'uuid']"

[[test.results]]
value = "['array', 'array']"

[[test.results]]
value = "['object', 'object']"

[[test.results]]
value = "['string', 'string']"

[[test.results]]
value = "['duration', 'duration']"

[[test.results]]
value = "['datetime', 'datetime']"

[[test.results]]
value = "['int', 'int']"

[[test.results]]
value = "['float', 'float']"

[[test.results]]
value = "['decimal', 'decimal']"

[[test.results]]
value = "['geometry<point>', 'geometry<point>']"

[[test.results]]
value = "['geometry<line>', 'geometry<line>']"

[[test.results]]
value = "['geometry<polygon>', 'geometry<polygon>']"

[[test.results]]
value = "['geometry<multipoint>', 'geometry<multipoint>']"

[[test.results]]
value = "['geometry<multiline>', 'geometry<multiline>']"

[[test.results]]
value = "['geometry<multipolygon>', 'geometry<multipolygon>']"

[[test.results]]
value = "['geometry<collection>', 'geometry<collection>']"

[[test.results]]
value = "['regex', 'regex']"

[[test.results]]
value = "['file', 'file']"

[[test.results]]
value = "['bytes', 'bytes']"

[[test.results]]
value = "['range', 'range']"

[[test.results]]
value = "['record', 'record']"

[[test.results]]
value = "['table', 'table']"

[[test.results]]
value = "'function'"

*/

-- Test NONE
RETURN [type::of(NONE), NONE.type_of()];

-- Test NULL
RETURN [type::of(NULL), NULL.type_of()];

-- Test boolean values
RETURN [type::of(true), true.type_of()];

-- Test UUID
RETURN [type::of(u"017f22e7-79bd-b7a8-988b-b1e0e34c221b"), u"017f22e7-79bd-b7a8-988b-b1e0e34c221b".type_of()];

-- Test array
RETURN [type::of([1, 2, 3]), [1, 2, 3].type_of()];

-- Test object
RETURN [type::of({ foo: "bar" }), { foo: "bar" }.type_of()];

-- Test string
RETURN [type::of("hello"), "hello".type_of()];

-- Test duration
RETURN [type::of(1h30m), 1h30m.type_of()];

-- Test datetime
RETURN [type::of(d"2023-10-24T12:00:00Z"), d"2023-10-24T12:00:00Z".type_of()];

-- Test numbers
RETURN [type::of(42), 42.type_of()];
RETURN [type::of(3.14), 3.14.type_of()];
RETURN [type::of(99.99dec), 99.99dec.type_of()];

-- Test geometry types
RETURN [type::of((0.0, 0.0)), (0.0, 0.0).type_of()];
RETURN [type::of({ type: "LineString", coordinates: [[0, 0], [1, 1]] }), { type: "LineString", coordinates: [[0, 0], [1, 1]] }.type_of()];
RETURN [type::of({ type: "Polygon", coordinates: [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]] }), { type: "Polygon", coordinates: [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]] }.type_of()];
RETURN [type::of({ type: "MultiPoint", coordinates: [[0, 0], [1, 1]] }), { type: "MultiPoint", coordinates: [[0, 0], [1, 1]] }.type_of()];
RETURN [type::of({ type: "MultiLineString", coordinates: [[[0, 0], [1, 1]], [[2, 2], [3, 3]]] }), { type: "MultiLineString", coordinates: [[[0, 0], [1, 1]], [[2, 2], [3, 3]]] }.type_of()];
RETURN [type::of({ type: "MultiPolygon", coordinates: [[[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]] }), { type: "MultiPolygon", coordinates: [[[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]] }.type_of()];
RETURN [type::of({ type: "GeometryCollection", geometries: [{ type: "Point", coordinates: [0, 0] }] }), { type: "GeometryCollection", geometries: [{ type: "Point", coordinates: [0, 0] }] }.type_of()];

-- Test regex
RETURN [type::of(/test/), /test/.type_of()];

-- Test file
RETURN [type::of(type::file("bucket", "key")), type::file("bucket", "key").type_of()];

-- Test bytes
RETURN [type::of(<bytes>"hello"), (<bytes>"hello").type_of()];

-- Test range
RETURN [type::of(1..10), (1..10).type_of()];

-- Test record
RETURN [type::of(user:123), user:123.type_of()];

-- Test table
RETURN [type::of(type::table("users")), type::table("users").type_of()];

-- Test function/closure
RETURN type::of(|| "test");
