/**
[env.capabilities]
allow-experimental = ["files"]

[test]
reason = "Test the type_of function for all value types"

[[test.results]]
value = "'none'"

[[test.results]]
value = "'null'"

[[test.results]]
value = "'bool'"

[[test.results]]
value = "'bool'"

[[test.results]]
value = "'uuid'"

[[test.results]]
value = "'array'"

[[test.results]]
value = "'object'"

[[test.results]]
value = "'string'"

[[test.results]]
value = "'duration'"

[[test.results]]
value = "'datetime'"

[[test.results]]
value = "'function'"

[[test.results]]
value = "'int'"

[[test.results]]
value = "'float'"

[[test.results]]
value = "'decimal'"

[[test.results]]
value = "'geometry<point>'"

[[test.results]]
value = "'geometry<line>'"

[[test.results]]
value = "'geometry<polygon>'"

[[test.results]]
value = "'geometry<multipoint>'"

[[test.results]]
value = "'geometry<multiline>'"

[[test.results]]
value = "'geometry<multipolygon>'"

[[test.results]]
value = "'geometry<collection>'"

[[test.results]]
value = "'regex'"

[[test.results]]
value = "'file'"

[[test.results]]
value = "'bytes'"

[[test.results]]
value = "'range'"

[[test.results]]
value = "'record'"

[[test.results]]
value = "'table'"

*/

-- Test NONE
RETURN type_of(NONE);

-- Test NULL
RETURN type_of(NULL);

-- Test boolean values
RETURN type_of(true);
RETURN type_of(false);

-- Test UUID
RETURN type_of(u"017f22e7-79bd-b7a8-988b-b1e0e34c221b");

-- Test array
RETURN type_of([1, 2, 3]);

-- Test object
RETURN type_of({ foo: "bar" });

-- Test string
RETURN type_of("hello");

-- Test duration
RETURN type_of(1h30m);

-- Test datetime
RETURN type_of(d"2023-10-24T12:00:00Z");

-- Test function/closure
RETURN type_of(|| "test");

-- Test numbers
RETURN type_of(42);
RETURN type_of(3.14);
RETURN type_of(99.99dec);

-- Test geometry types
RETURN type_of((0.0, 0.0));
RETURN type_of({ type: "LineString", coordinates: [[0, 0], [1, 1]] });
RETURN type_of({ type: "Polygon", coordinates: [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]] });
RETURN type_of({ type: "MultiPoint", coordinates: [[0, 0], [1, 1]] });
RETURN type_of({ type: "MultiLineString", coordinates: [[[0, 0], [1, 1]], [[2, 2], [3, 3]]] });
RETURN type_of({ type: "MultiPolygon", coordinates: [[[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]] });
RETURN type_of({ type: "GeometryCollection", geometries: [{ type: "Point", coordinates: [0, 0] }] });

-- Test regex
RETURN type_of(/test/);

-- Test file
RETURN type_of(type::file("bucket", "key"));

-- Test bytes
RETURN type_of(<bytes>"hello");

-- Test range
RETURN type_of(1..10);

-- Test record
RETURN type_of(user:123);

-- Test table
RETURN type_of(type::table("users"));

