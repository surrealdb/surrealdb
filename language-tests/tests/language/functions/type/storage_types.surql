/**
[env.capabilities]
allow-experimental = ["files"]

[test]
reason = "Test that all Value storage types are preserved correctly when stored and retrieved from the database"

[[test.results]]
value = "'none'"

[[test.results]]
value = "'null'"

[[test.results]]
value = "'bool'"

[[test.results]]
value = "'uuid'"

[[test.results]]
value = "'array'"

[[test.results]]
value = "'set'"

[[test.results]]
value = "'object'"

[[test.results]]
value = "'string'"

[[test.results]]
value = "'duration'"

[[test.results]]
value = "'datetime'"

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

-- Test NONE storage
{
    CREATE test_values:none SET value = NONE;
    RETURN (SELECT VALUE type::of(value) FROM test_values:none)[0];
};

-- Test NULL storage
{
    CREATE test_values:null SET value = NULL;
    RETURN (SELECT VALUE type::of(value) FROM test_values:null)[0];
};

-- Test boolean storage
{
    CREATE test_values:bool SET value = true;
    RETURN (SELECT VALUE type::of(value) FROM test_values:bool)[0];
};

-- Test UUID storage
{
    CREATE test_values:uuid SET value = u"017f22e7-79bd-b7a8-988b-b1e0e34c221b";
    RETURN (SELECT VALUE type::of(value) FROM test_values:uuid)[0];
};

-- Test array storage
{
    CREATE test_values:array SET value = [1, 2, 3];
    RETURN (SELECT VALUE type::of(value) FROM test_values:array)[0];
};

-- Test set storage
{
    CREATE test_values:set SET value = type::set([1, 2, 3]);
    RETURN (SELECT VALUE type::of(value) FROM test_values:set)[0];
};

-- Test object storage
{
    CREATE test_values:object SET value = { foo: "bar" };
    RETURN (SELECT VALUE type::of(value) FROM test_values:object)[0];
};

-- Test string storage
{
    CREATE test_values:string SET value = "hello";
    RETURN (SELECT VALUE type::of(value) FROM test_values:string)[0];
};

-- Test duration storage
{
    CREATE test_values:duration SET value = 1h30m;
    RETURN (SELECT VALUE type::of(value) FROM test_values:duration)[0];
};

-- Test datetime storage
{
    CREATE test_values:datetime SET value = d"2023-10-24T12:00:00Z";
    RETURN (SELECT VALUE type::of(value) FROM test_values:datetime)[0];
};

-- Test int storage
{
    CREATE test_values:int SET value = 42;
    RETURN (SELECT VALUE type::of(value) FROM test_values:int)[0];
};

-- Test float storage
{
    CREATE test_values:float SET value = 3.14;
    RETURN (SELECT VALUE type::of(value) FROM test_values:float)[0];
};

-- Test decimal storage
{
    CREATE test_values:decimal SET value = 99.99dec;
    RETURN (SELECT VALUE type::of(value) FROM test_values:decimal)[0];
};

-- Test geometry point storage
{
    CREATE test_values:geo_point SET value = (0.0, 0.0);
    RETURN (SELECT VALUE type::of(value) FROM test_values:geo_point)[0];
};

-- Test geometry line storage
{
    CREATE test_values:geo_line SET value = { type: "LineString", coordinates: [[0, 0], [1, 1]] };
    RETURN (SELECT VALUE type::of(value) FROM test_values:geo_line)[0];
};

-- Test geometry polygon storage
{
    CREATE test_values:geo_polygon SET value = { type: "Polygon", coordinates: [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]] };
    RETURN (SELECT VALUE type::of(value) FROM test_values:geo_polygon)[0];
};

-- Test geometry multipoint storage
{
    CREATE test_values:geo_multipoint SET value = { type: "MultiPoint", coordinates: [[0, 0], [1, 1]] };
    RETURN (SELECT VALUE type::of(value) FROM test_values:geo_multipoint)[0];
};

-- Test geometry multiline storage
{
    CREATE test_values:geo_multiline SET value = { type: "MultiLineString", coordinates: [[[0, 0], [1, 1]], [[2, 2], [3, 3]]] };
    RETURN (SELECT VALUE type::of(value) FROM test_values:geo_multiline)[0];
};

-- Test geometry multipolygon storage
{
    CREATE test_values:geo_multipolygon SET value = { type: "MultiPolygon", coordinates: [[[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]] };
    RETURN (SELECT VALUE type::of(value) FROM test_values:geo_multipolygon)[0];
};

-- Test geometry collection storage
{
    CREATE test_values:geo_collection SET value = { type: "GeometryCollection", geometries: [{ type: "Point", coordinates: [0, 0] }] };
    RETURN (SELECT VALUE type::of(value) FROM test_values:geo_collection)[0];
};

-- Test regex storage
{
    CREATE test_values:regex SET value = /test/;
    RETURN (SELECT VALUE type::of(value) FROM test_values:regex)[0];
};

-- Test file storage
{
    CREATE test_values:file SET value = type::file("bucket", "key");
    RETURN (SELECT VALUE type::of(value) FROM test_values:file)[0];
};

-- Test bytes storage
{
    CREATE test_values:bytes SET value = <bytes>"hello";
    RETURN (SELECT VALUE type::of(value) FROM test_values:bytes)[0];
};

-- Test range storage
{
    CREATE test_values:range SET value = 1..10;
    RETURN (SELECT VALUE type::of(value) FROM test_values:range)[0];
};

-- Test record storage
{
    CREATE test_values:record SET value = user:123;
    RETURN (SELECT VALUE type::of(value) FROM test_values:record)[0];
};

-- Test table storage
{
    CREATE test_values:table SET value = type::table("users");
    RETURN (SELECT VALUE type::of(value) FROM test_values:table)[0];
};
