@0x9f533b07ce5fe0a3;

struct BtreeValueMap {
    items @0 :List(BtreeValueMap.Item);

    struct Item {
        key @0 :Text;
        value @1 :Value;
    }
}

struct Duration {
    seconds @0 :UInt64;  # Duration in seconds.
    nanos @1 :UInt32;    # Additional nanoseconds.
}
struct Timestamp {
    seconds @0 :Int64;  # Datetime in seconds.
    nanos @1 :UInt32;    # Additional nanoseconds.
}
struct Array {
    values @0 :List(Value);  # List of values in the array.
}
struct Object {
    map @0 :BtreeValueMap;  # Map of key-value pairs in the object.
}
struct Uuid {
    bytes @0 :Data;
}

struct Geometry {
    union {
        point @0 :Point;  # Point geometry.
        line @1 :LineString;  # Line string geometry.
        polygon @2 :Polygon;  # Polygon geometry.
        multiPoint @3 :MultiPoint;  # Multi-point geometry.
        multiLine @4 :MultiLineString;  # Multi-line string geometry.
        multiPolygon @5 :MultiPolygon;  # Multi-polygon geometry.
        collection @6 :GeometryCollection;  # Geometry collection.
    }

    struct Point {
        x @0 :Float64;  # X coordinate of the point.
        y @1 :Float64;  # Y coordinate of the point.
    }
    struct LineString {
        points @0 :List(Point);  # List of points in the line string.
    }
    struct Polygon {
        exterior @0 :LineString;  # Exterior line string of the polygon.
        interiors @1 :List(LineString);  # List of interior line strings (holes).
    }
    struct MultiPoint {
        points @0 :List(Point);  # List of points in the multi-point geometry.
    }
    struct MultiLineString {
        lines @0 :List(LineString);  # List of line strings in the multi-line geometry.
    }
    struct MultiPolygon {
        polygons @0 :List(Polygon);  # List of polygons in the multi-polygon geometry.
    }
    struct GeometryCollection {
        geometries @0 :List(Geometry);  # List of geometries in the collection.
    }
}

struct RecordId {
    table @0 :Text;  # Name of the table.
    id @1 :Id;  # Identifier of the record.
}
struct Id {
    union {
        number @0 :Int64;  # Numeric identifier.
        string @1 :Text;  # String identifier.
        uuid @2 :Uuid;  # UUID identifier.
        array @3 :Array;  # Array identifier.
    }
}

struct File {
    bucket @0 :Text;  # Bucket name for the file.
    key @1 :Text;  # Key of the file.
}
struct Resource {
    union {
        table @0 :Text;  # Name of the table.
        recordId @1 :RecordId;  # Record identifier.
        object @2 :Object;  # Object resource.
        array @3 :Array;  # Array resource.
        # edge @4 :Edge;  # Edge resource (not implemented).
    }
}
struct Value {
    union {
        null @0 :Void;  # Represents a null value.
        bool @1 :Bool;  # Boolean value.
        int64 @2 :Int64;  # Integer value.
        float64 @3 :Float64;  # Floating-point value.
        decimal @4 :Text;  # Decimal value as a string.
        string @5 :Text;  # String value.
        duration @6 :Duration;  # Duration value.
        datetime @7 :Timestamp;  # Datetime value.
        uuid @8 :Uuid;  # UUID value as a string.
        array @9 :Array;  # Array value.
        object @10 :Object;  # Object value.
        geometry @11 :Geometry;  # Geometry value.
        bytes @12 :Data;  # Bytes value.
        recordId @13 :RecordId;  # Record identifier.
        file @14 :File;  # File resource.
    }
}


# Other types that may be moved to their own files in the future:

struct Fields {
    single @0 :Bool;  # Indicates if the fields are single-valued.
    fields @1 :List(Field);  # List of fields.
}

struct Field {
    union {
        all @0 :Void;  # Represents all fields.
        single @1 :Field.Single;  # Represents a single field.
    }

    struct Single {
        name @0 :Text;  # Name of the single field.
        alias @1 :Text;  # Alias for the field.
    }
}