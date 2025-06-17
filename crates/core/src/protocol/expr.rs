use crate::protocol::{ToCapnp, FromCapnp};

use crate::expr::{Array, Datetime, Duration, File, Geometry, Id, Number, Object, Strand, Thing, Uuid, Value};
use chrono::{DateTime, Utc};
use geo::Point;
use rust_decimal::Decimal;
use core::panic;
use std::collections::BTreeMap;

use crate::protocol::expr_capnp;

impl ToCapnp for Value {
    type Builder<'a> = expr_capnp::value::Builder<'a>;

    fn to_capnp(&self, mut builder: Self::Builder<'_>) {
        match self {
            Value::Null => builder.set_null(()),
            Value::Bool(b) => builder.set_bool(*b),
            Value::Number(n) => {
                match n {
                    crate::expr::Number::Int(i) => builder.set_int64(*i),
                    crate::expr::Number::Float(f) => builder.set_float64(*f),
                    crate::expr::Number::Decimal(d) => builder.set_decimal(d.to_string().as_str()),
                }
            }
            Value::Strand(s) => builder.set_string(s.as_str()),
            Value::Bytes(b) => builder.set_bytes(b.as_slice()),
            Value::Thing(thing) => {
                let record_id_builder = builder.init_record_id();
                thing.to_capnp(record_id_builder);
            }
            Value::Duration(d) => {
                let duration_builder = builder.init_duration();
                d.to_capnp(duration_builder);
            }
            Value::Datetime(dt) => {
                let datetime_builder = builder.init_datetime();
                dt.to_capnp(datetime_builder);
            }
            Value::Uuid(uuid) => {
                let uuid_builder = builder.init_uuid();
                uuid.to_capnp(uuid_builder);
            }
            Value::Object(obj) => {
                let object_builder = builder.init_object();
                obj.to_capnp(object_builder);
            }
            Value::Array(arr) => {
                let array_builder = builder.init_array();
                arr.to_capnp(array_builder);
            },
            _ => {
                // TODO: DO NOT PANIC, we just need to modify the Value enum which Mees is currently working on.
                panic!("Unsupported value type for Cap'n Proto serialization: {:?}", self);
            }
        }
    }
}


impl FromCapnp for Value {
    type Reader<'a> = expr_capnp::value::Reader<'a>;

    fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
        match reader.which()? {
            expr_capnp::value::Which::Null(()) => Ok(Value::Null),
            expr_capnp::value::Which::Bool(b) => Ok(Value::Bool(b)),
            expr_capnp::value::Which::Int64(i) => Ok(Value::Number(Number::Int(i))),
            expr_capnp::value::Which::Float64(f) => Ok(Value::Number(Number::Float(f))),
            expr_capnp::value::Which::Decimal(d) => {
                // TODO: Do not send decimals as strings so that we can avoid parsing.
                let decimal = d?.to_string()?.as_str().parse::<Decimal>()
                    .map_err(|_| ::capnp::Error::failed("Invalid decimal format".to_string()))?;

                Ok(Value::Number(Number::Decimal(decimal)))
            }
            expr_capnp::value::Which::String(s) => Ok(Value::Strand(Strand(s?.to_string()?))),
            expr_capnp::value::Which::Bytes(b) => Ok(Value::Bytes(crate::expr::Bytes(b?.to_vec()))),
            expr_capnp::value::Which::Duration(d) => Ok(Value::Duration(Duration::from_capnp(d?)?)),
            expr_capnp::value::Which::Datetime(t) => Ok(Value::Datetime(Datetime(DateTime::<Utc>::from_capnp(t?)?))),
            expr_capnp::value::Which::RecordId(t) => Ok(Value::Thing(Thing::from_capnp(t?)?)),
            expr_capnp::value::Which::File(f) => Ok(Value::File(File::from_capnp(f?)?)),
            expr_capnp::value::Which::Uuid(u) => Ok(Value::Uuid(Uuid::from_capnp(u?)?)),
            expr_capnp::value::Which::Object(o) => Ok(Value::Object(Object::from_capnp(o?)?)),
            expr_capnp::value::Which::Array(a) => Ok(Value::Array(Array::from_capnp(a?)?)),
            expr_capnp::value::Which::Geometry(geometry) => Ok(Value::Geometry(Geometry::from_capnp(geometry?)?)),
        }
    }
}


impl ToCapnp for Duration {
    type Builder<'a> = expr_capnp::duration::Builder<'a>;
    
    fn to_capnp(&self, mut builder: Self::Builder<'_>) {
        builder.set_seconds(self.as_secs());
        builder.set_nanos(self.subsec_nanos());
    }
}

impl FromCapnp for Duration {
    type Reader<'a> = expr_capnp::duration::Reader<'a>;

    fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
        let seconds = reader.get_seconds();
        let nanos = reader.get_nanos();
        Ok(Duration::new(seconds as u64, nanos as u32))
    }
}

impl ToCapnp for DateTime<Utc> {
    type Builder<'a> = expr_capnp::timestamp::Builder<'a>;

    fn to_capnp(&self, mut builder: Self::Builder<'_>) {
        builder.set_seconds(self.timestamp());
        builder.set_nanos(self.timestamp_subsec_nanos());
    }
}

impl FromCapnp for DateTime<Utc> {
    type Reader<'a> = expr_capnp::timestamp::Reader<'a>;

    fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
        let seconds = reader.get_seconds();
        let nanos = reader.get_nanos() as u32;
        let dt = DateTime::<Utc>::from_timestamp(seconds, nanos)
            .ok_or_else(|| ::capnp::Error::failed("Invalid timestamp".to_string()))?;
        Ok(dt)
    }
}

impl ToCapnp for Uuid {
    type Builder<'a> = expr_capnp::uuid::Builder<'a>;

    fn to_capnp(&self, mut builder: Self::Builder<'_>) {
        builder.set_bytes(self.as_bytes());
    }
}

impl FromCapnp for Uuid {
    type Reader<'a> = expr_capnp::uuid::Reader<'a>;

    fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
        let bytes = reader.get_bytes()?;
        Uuid::from_slice(bytes).map_err(|_| ::capnp::Error::failed("Invalid UUID".to_string()))
    }
}

impl ToCapnp for Thing {
    type Builder<'a> = expr_capnp::record_id::Builder<'a>;

    fn to_capnp(&self, mut builder: Self::Builder<'_>) {
        builder.set_table(self.tb.as_str());
        let id_builder = builder.init_id();
        self.id.to_capnp(id_builder);
    }
}

impl FromCapnp for Thing {
    type Reader<'a> = expr_capnp::record_id::Reader<'a>;

    fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
        let table = reader.get_table()?.to_string()?;
        let id_reader = reader.get_id()?;
        let id = Id::from_capnp(id_reader)?;
        Ok(Thing {
            tb: table,
            id,
        })
    }
}

impl ToCapnp for Id {
    type Builder<'a> = expr_capnp::id::Builder<'a>;

    fn to_capnp(&self, mut builder: Self::Builder<'_>) {
        match self {
            Self::Number(n) => builder.set_number(*n),
            Self::String(s) => builder.set_string(s.as_str()),
            Self::Uuid(uuid) => {
                let uuid_builder = builder.init_uuid();
                uuid.to_capnp(uuid_builder);
            }
            Self::Array(arr) => {
                let array_builder = builder.init_array();
                arr.to_capnp(array_builder);
            }
            _ => {
                // TODO: DO NOT PANIC, we just need to modify the Id enum.
                panic!("Unsupported Id type for Cap'n Proto serialization: {:?}", self);
            }
        }
    }
}

impl FromCapnp for Id {
    type Reader<'a> = expr_capnp::id::Reader<'a>;

    fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
        match reader.which()? {
            expr_capnp::id::Which::Number(n) => Ok(Id::Number(n)),
            expr_capnp::id::Which::String(s) => Ok(Id::String(s?.to_string()?)),
            expr_capnp::id::Which::Uuid(u) => {
                let uuid = Uuid::from_capnp(u?)?;
                Ok(Id::Uuid(uuid))
            }
            expr_capnp::id::Which::Array(a) => {
                let array = Array::from_capnp(a?)?;
                Ok(Id::Array(array))
            }
            _ => Err(::capnp::Error::failed("Unsupported Id type".to_string())),
        }
    }
}

impl ToCapnp for File {
    type Builder<'a> = expr_capnp::file::Builder<'a>;

    fn to_capnp(&self, mut builder: Self::Builder<'_>) {
        builder.set_bucket(self.bucket.as_str());
        builder.set_key(self.key.as_str());
    }
}

impl FromCapnp for File {
    type Reader<'a> = expr_capnp::file::Reader<'a>;

    fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
        let bucket = reader.get_bucket()?.to_string()?;
        let key = reader.get_key()?.to_string()?;
        Ok(File { bucket, key })
    }
}

impl ToCapnp for BTreeMap<String, Value> {
    type Builder<'a> = expr_capnp::btree_value_map::Builder<'a>;

    fn to_capnp(&self, builder: Self::Builder<'_>) {
        let mut items_builder = builder.init_items(self.len() as u32);
        for (index, (key, value)) in self.iter().enumerate() {
            let mut entry_builder = items_builder.reborrow().get(index as u32);
            entry_builder.set_key(key.as_str());
            let value_builder = entry_builder.init_value();
            value.to_capnp(value_builder);
        }
    }
}

impl FromCapnp for BTreeMap<String, Value> {
    type Reader<'a> = expr_capnp::btree_value_map::Reader<'a>;

    fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
        let mut map = BTreeMap::new();
        for entry in reader.get_items()? {
            let key = entry.get_key()?.to_string()?;
            let value = Value::from_capnp(entry.get_value()?)?;
            map.insert(key, value);
        }
        Ok(map)
    }
}

impl ToCapnp for Object {
    type Builder<'a> = expr_capnp::object::Builder<'a>;

    fn to_capnp(&self, builder: Self::Builder<'_>) {
        let mut btree_map_builder = builder.init_map();
        self.0.to_capnp(btree_map_builder);
    }
}

impl FromCapnp for Object {
    type Reader<'a> = expr_capnp::object::Reader<'a>;

    fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
        let btree_map_reader = reader.get_map()?;
        let map = BTreeMap::from_capnp(btree_map_reader)?;
        Ok(Object(map))
    }
}

impl ToCapnp for Array {
    type Builder<'a> = expr_capnp::array::Builder<'a>;

    fn to_capnp(&self, builder: Self::Builder<'_>) {
        let mut list_builder = builder.init_values(self.0.len() as u32);
        for (index, value) in self.0.iter().enumerate() {
            let item_builder = list_builder.reborrow().get(index as u32);
            value.to_capnp(item_builder);
        }
    }
}

impl FromCapnp for Array {
    type Reader<'a> = expr_capnp::array::Reader<'a>;

    fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
        let mut vec = Vec::new();

        for item in reader.get_values()? {
            vec.push(Value::from_capnp(item)?);
        }
        Ok(Array(vec))
    }
}

impl ToCapnp for Geometry {
    type Builder<'a> = expr_capnp::geometry::Builder<'a>;

    fn to_capnp(&self, mut builder: Self::Builder<'_>) {
        match self {
            Geometry::Point(point) => {
                let point_builder = builder.init_point();
                point.to_capnp(point_builder);
            }
            Geometry::Line(line_string) => {
                let line_string_builder = builder.init_line();
                line_string.to_capnp(line_string_builder);
            }
            Geometry::Polygon(polygon) => {
                let polygon_builder = builder.init_polygon();
                polygon.to_capnp(polygon_builder);
            }
            Geometry::MultiPoint(multi_point) => {
                let multi_point_builder = builder.init_multi_point();
                multi_point.to_capnp(multi_point_builder);
            }
            Geometry::MultiLine(multi_line_string) => {
                let multi_line_string_builder = builder.init_multi_line();
                multi_line_string.to_capnp(multi_line_string_builder);
            }
            Geometry::MultiPolygon(multi_polygon) => {
                let multi_polygon_builder = builder.init_multi_polygon();
                multi_polygon.to_capnp(multi_polygon_builder);
            }
            Geometry::Collection(geometries) => {
                let geometry_collection_builder = builder.init_collection();
                let mut geometries_builder = geometry_collection_builder.init_geometries(geometries.len() as u32);
                for (index, geometry) in geometries.iter().enumerate() {
                    let geometry_builder = geometries_builder.reborrow().get(index as u32);
                    geometry.to_capnp(geometry_builder);
                }
            }
        }
    }
}

impl FromCapnp for Geometry {
    type Reader<'a> = expr_capnp::geometry::Reader<'a>;

    fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
        match reader.which()? {
            expr_capnp::geometry::Which::Point(point) => Ok(Geometry::Point(geo::Point::from_capnp(point?)?)),
            expr_capnp::geometry::Which::Line(line_string) => Ok(Geometry::Line(geo::LineString::from_capnp(line_string?)?)),
            expr_capnp::geometry::Which::Polygon(polygon) => Ok(Geometry::Polygon(geo::Polygon::from_capnp(polygon?)?)),
            expr_capnp::geometry::Which::MultiPoint(multi_point) => Ok(Geometry::MultiPoint(geo::MultiPoint::from_capnp(multi_point?)?)),
            expr_capnp::geometry::Which::MultiLine(multi_line_string) => Ok(Geometry::MultiLine(geo::MultiLineString::from_capnp(multi_line_string?)?)),
            expr_capnp::geometry::Which::MultiPolygon(multi_polygon) => Ok(Geometry::MultiPolygon(geo::MultiPolygon::from_capnp(multi_polygon?)?)),
            expr_capnp::geometry::Which::Collection(geometry_collection) => {
                let geometry_collection = geometry_collection?;
                let geometries_reader = geometry_collection.get_geometries()?;
                let mut geometries = Vec::with_capacity(geometries_reader.len() as usize);
                for geometry in geometries_reader {
                    geometries.push(Geometry::from_capnp(geometry)?);
                }
                Ok(Geometry::Collection(geometries))
            }
        }
    }
}

impl ToCapnp for geo::Point {
    type Builder<'a> = expr_capnp::geometry::point::Builder<'a>;

    fn to_capnp(&self, mut builder: Self::Builder<'_>) {
        builder.set_x(self.x());
        builder.set_y(self.y());
    }
}

impl FromCapnp for geo::Point {
    type Reader<'a> = expr_capnp::geometry::point::Reader<'a>;

    fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
        let x = reader.get_x();
        let y = reader.get_y();
        Ok(Self::new(x, y))
    }
}

impl ToCapnp for geo::Coord {
    type Builder<'a> = expr_capnp::geometry::point::Builder<'a>;

    fn to_capnp(&self, mut builder: Self::Builder<'_>) {
        builder.set_x(self.x);
        builder.set_y(self.y);
    }
}
impl FromCapnp for geo::Coord {
    type Reader<'a> = expr_capnp::geometry::point::Reader<'a>;

    fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
        let x = reader.get_x();
        let y = reader.get_y();
        Ok(Self { x, y })
    }
}

impl ToCapnp for geo::LineString {
    type Builder<'a> = expr_capnp::geometry::line_string::Builder<'a>;

    fn to_capnp(&self, mut builder: Self::Builder<'_>) {
        let mut points_builder = builder.init_points(self.0.len() as u32);
        for (index, point) in self.0.iter().enumerate() {
            let point_builder = points_builder.reborrow().get(index as u32);
            point.to_capnp(point_builder);
        }
    }
}

impl FromCapnp for geo::LineString {
    type Reader<'a> = expr_capnp::geometry::line_string::Reader<'a>;

    fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
        let mut points = Vec::new();
        for point in reader.get_points()? {
            points.push(geo::Coord::from_capnp(point)?);
        }
        Ok(Self(points))
    }
}

impl ToCapnp for geo::Polygon {
    type Builder<'a> = expr_capnp::geometry::polygon::Builder<'a>;

    fn to_capnp(&self, mut builder: Self::Builder<'_>) {
        let mut exterior_builder = builder.reborrow().init_exterior();
        self.exterior().to_capnp(exterior_builder);

        let interiors = self.interiors();
        let mut interiors_builder = builder.reborrow().init_interiors(interiors.len() as u32);
        for (index, interior) in interiors.iter().enumerate() {
            let interior_builder = interiors_builder.reborrow().get(index as u32);
            interior.to_capnp(interior_builder);
        }
    }
}

impl FromCapnp for geo::Polygon {
    type Reader<'a> = expr_capnp::geometry::polygon::Reader<'a>;

    fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
        let exterior = reader.get_exterior()?;
        let exterior = geo::LineString::from_capnp(exterior)?;
        
        let mut interiors = Vec::new();
        for interior in reader.get_interiors()? {
            interiors.push(geo::LineString::from_capnp(interior)?);
        }
        
        Ok(Self::new(exterior, interiors))
    }
}


impl ToCapnp for geo::MultiPoint {
    type Builder<'a> = expr_capnp::geometry::multi_point::Builder<'a>;

    fn to_capnp(&self, mut builder: Self::Builder<'_>) {
        let mut points_builder = builder.init_points(self.0.len() as u32);
        for (index, point) in self.0.iter().enumerate() {
            let point_builder = points_builder.reborrow().get(index as u32);
            point.to_capnp(point_builder);
        }
    }
}
impl FromCapnp for geo::MultiPoint {
    type Reader<'a> = expr_capnp::geometry::multi_point::Reader<'a>;

    fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
        let mut points = Vec::new();
        for point in reader.get_points()? {
            points.push(geo::Point::from_capnp(point)?);
        }
        Ok(Self(points))
    }
}
impl ToCapnp for geo::MultiLineString {
    type Builder<'a> = expr_capnp::geometry::multi_line_string::Builder<'a>;

    fn to_capnp(&self, mut builder: Self::Builder<'_>) {
        let mut lines_builder = builder.init_lines(self.0.len() as u32);
        for (index, line) in self.0.iter().enumerate() {
            let line_builder = lines_builder.reborrow().get(index as u32);
            line.to_capnp(line_builder);
        }
    }
}
impl FromCapnp for geo::MultiLineString {
    type Reader<'a> = expr_capnp::geometry::multi_line_string::Reader<'a>;

    fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
        let mut lines = Vec::new();
        for line in reader.get_lines()? {
            lines.push(geo::LineString::from_capnp(line)?);
        }
        Ok(Self(lines))
    }
}
impl ToCapnp for geo::MultiPolygon {
    type Builder<'a> = expr_capnp::geometry::multi_polygon::Builder<'a>;

    fn to_capnp(&self, mut builder: Self::Builder<'_>) {
        let mut polygons_builder = builder.init_polygons(self.0.len() as u32);
        for (index, polygon) in self.0.iter().enumerate() {
            let polygon_builder = polygons_builder.reborrow().get(index as u32);
            polygon.to_capnp(polygon_builder);
        }
    }
}
impl FromCapnp for geo::MultiPolygon {
    type Reader<'a> = expr_capnp::geometry::multi_polygon::Reader<'a>;

    fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
        let mut polygons = Vec::new();
        for polygon in reader.get_polygons()? {
            polygons.push(geo::Polygon::from_capnp(polygon)?);
        }
        Ok(Self(polygons))
    }
}
