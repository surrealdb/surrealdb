
use crate::protocol::{FromCapnp, FromFlatbuffers, ToCapnp, ToFlatbuffers};

use crate::expr::{Array, Datetime, Duration, File, Geometry, Id, Number, Object, Strand, Thing, Uuid, Value};
use anyhow::{anyhow, Context};
use chrono::{DateTime, Utc};
use geo::Point;
use rust_decimal::Decimal;
use core::panic;
use std::collections::BTreeMap;


use crate::protocol::flatbuffers::surreal_db::protocol::expr as expr_fb;
use crate::protocol::flatbuffers::surreal_db::protocol::common as common_fb;

// impl ToCapnp for Value {
//     type Builder<'a> = expr_capnp::value::Builder<'a>;

//     fn to_capnp(&self, mut builder: Self::Builder<'_>) {
//         match self {
//             Value::Null => builder.set_null(()),
//             Value::Bool(b) => builder.set_bool(*b),
//             Value::Number(n) => {
//                 match n {
//                     crate::expr::Number::Int(i) => builder.set_int64(*i),
//                     crate::expr::Number::Float(f) => builder.set_float64(*f),
//                     crate::expr::Number::Decimal(d) => builder.set_decimal(d.to_string().as_str()),
//                 }
//             }
//             Value::Strand(s) => builder.set_string(s.as_str()),
//             Value::Bytes(b) => builder.set_bytes(b.as_slice()),
//             Value::Thing(thing) => {
//                 let record_id_builder = builder.init_record_id();
//                 thing.to_capnp(record_id_builder);
//             }
//             Value::Duration(d) => {
//                 let duration_builder = builder.init_duration();
//                 d.to_capnp(duration_builder);
//             }
//             Value::Datetime(dt) => {
//                 let datetime_builder = builder.init_datetime();
//                 dt.to_capnp(datetime_builder);
//             }
//             Value::Uuid(uuid) => {
//                 let uuid_builder = builder.init_uuid();
//                 uuid.to_capnp(uuid_builder);
//             }
//             Value::Object(obj) => {
//                 let object_builder = builder.init_object();
//                 obj.to_capnp(object_builder);
//             }
//             Value::Array(arr) => {
//                 let array_builder = builder.init_array();
//                 arr.to_capnp(array_builder);
//             },
//             _ => {
//                 // TODO: DO NOT PANIC, we just need to modify the Value enum which Mees is currently working on.
//                 panic!("Unsupported value type for Cap'n Proto serialization: {:?}", self);
//             }
//         }
//     }
// }


// impl FromCapnp for Value {
//     type Reader<'a> = expr_capnp::value::Reader<'a>;

//     fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
//         match reader.which()? {
//             expr_capnp::value::Which::Null(()) => Ok(Value::Null),
//             expr_capnp::value::Which::Bool(b) => Ok(Value::Bool(b)),
//             expr_capnp::value::Which::Int64(i) => Ok(Value::Number(Number::Int(i))),
//             expr_capnp::value::Which::Float64(f) => Ok(Value::Number(Number::Float(f))),
//             expr_capnp::value::Which::Decimal(d) => {
//                 // TODO: Do not send decimals as strings so that we can avoid parsing.
//                 let decimal = d?.to_string()?.as_str().parse::<Decimal>()
//                     .map_err(|_| ::capnp::Error::failed("Invalid decimal format".to_string()))?;

//                 Ok(Value::Number(Number::Decimal(decimal)))
//             }
//             expr_capnp::value::Which::String(s) => Ok(Value::Strand(Strand(s?.to_string()?))),
//             expr_capnp::value::Which::Bytes(b) => Ok(Value::Bytes(crate::expr::Bytes(b?.to_vec()))),
//             expr_capnp::value::Which::Duration(d) => Ok(Value::Duration(Duration::from_capnp(d?)?)),
//             expr_capnp::value::Which::Datetime(t) => Ok(Value::Datetime(Datetime(DateTime::<Utc>::from_capnp(t?)?))),
//             expr_capnp::value::Which::RecordId(t) => Ok(Value::Thing(Thing::from_capnp(t?)?)),
//             expr_capnp::value::Which::File(f) => Ok(Value::File(File::from_capnp(f?)?)),
//             expr_capnp::value::Which::Uuid(u) => Ok(Value::Uuid(Uuid::from_capnp(u?)?)),
//             expr_capnp::value::Which::Object(o) => Ok(Value::Object(Object::from_capnp(o?)?)),
//             expr_capnp::value::Which::Array(a) => Ok(Value::Array(Array::from_capnp(a?)?)),
//             expr_capnp::value::Which::Geometry(geometry) => Ok(Value::Geometry(Geometry::from_capnp(geometry?)?)),
//         }
//     }
// }


// impl ToCapnp for Duration {
//     type Builder<'a> = expr_capnp::duration::Builder<'a>;
    
//     fn to_capnp(&self, mut builder: Self::Builder<'_>) {
//         builder.set_seconds(self.as_secs());
//         builder.set_nanos(self.subsec_nanos());
//     }
// }

// impl FromCapnp for Duration {
//     type Reader<'a> = expr_capnp::duration::Reader<'a>;

//     fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
//         let seconds = reader.get_seconds();
//         let nanos = reader.get_nanos();
//         Ok(Duration::new(seconds as u64, nanos as u32))
//     }
// }

// impl ToCapnp for DateTime<Utc> {
//     type Builder<'a> = expr_capnp::timestamp::Builder<'a>;

//     fn to_capnp(&self, mut builder: Self::Builder<'_>) {
//         builder.set_seconds(self.timestamp());
//         builder.set_nanos(self.timestamp_subsec_nanos());
//     }
// }

// impl FromCapnp for DateTime<Utc> {
//     type Reader<'a> = expr_capnp::timestamp::Reader<'a>;

//     fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
//         let seconds = reader.get_seconds();
//         let nanos = reader.get_nanos() as u32;
//         let dt = DateTime::<Utc>::from_timestamp(seconds, nanos)
//             .ok_or_else(|| ::capnp::Error::failed("Invalid timestamp".to_string()))?;
//         Ok(dt)
//     }
// }

// impl ToCapnp for Uuid {
//     type Builder<'a> = expr_capnp::uuid::Builder<'a>;

//     fn to_capnp(&self, mut builder: Self::Builder<'_>) {
//         builder.set_bytes(self.as_bytes());
//     }
// }

// impl FromCapnp for Uuid {
//     type Reader<'a> = expr_capnp::uuid::Reader<'a>;

//     fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
//         let bytes = reader.get_bytes()?;
//         Uuid::from_slice(bytes).map_err(|_| ::capnp::Error::failed("Invalid UUID".to_string()))
//     }
// }

// impl ToCapnp for Thing {
//     type Builder<'a> = expr_capnp::record_id::Builder<'a>;

//     fn to_capnp(&self, mut builder: Self::Builder<'_>) {
//         builder.set_table(self.tb.as_str());
//         let id_builder = builder.init_id();
//         self.id.to_capnp(id_builder);
//     }
// }

// impl FromCapnp for Thing {
//     type Reader<'a> = expr_capnp::record_id::Reader<'a>;

//     fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
//         let table = reader.get_table()?.to_string()?;
//         let id_reader = reader.get_id()?;
//         let id = Id::from_capnp(id_reader)?;
//         Ok(Thing {
//             tb: table,
//             id,
//         })
//     }
// }

// impl ToCapnp for Id {
//     type Builder<'a> = expr_capnp::id::Builder<'a>;

//     fn to_capnp(&self, mut builder: Self::Builder<'_>) {
//         match self {
//             Self::Number(n) => builder.set_number(*n),
//             Self::String(s) => builder.set_string(s.as_str()),
//             Self::Uuid(uuid) => {
//                 let uuid_builder = builder.init_uuid();
//                 uuid.to_capnp(uuid_builder);
//             }
//             Self::Array(arr) => {
//                 let array_builder = builder.init_array();
//                 arr.to_capnp(array_builder);
//             }
//             _ => {
//                 // TODO: DO NOT PANIC, we just need to modify the Id enum.
//                 panic!("Unsupported Id type for Cap'n Proto serialization: {:?}", self);
//             }
//         }
//     }
// }

// impl FromCapnp for Id {
//     type Reader<'a> = expr_capnp::id::Reader<'a>;

//     fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
//         match reader.which()? {
//             expr_capnp::id::Which::Number(n) => Ok(Id::Number(n)),
//             expr_capnp::id::Which::String(s) => Ok(Id::String(s?.to_string()?)),
//             expr_capnp::id::Which::Uuid(u) => {
//                 let uuid = Uuid::from_capnp(u?)?;
//                 Ok(Id::Uuid(uuid))
//             }
//             expr_capnp::id::Which::Array(a) => {
//                 let array = Array::from_capnp(a?)?;
//                 Ok(Id::Array(array))
//             }
//             _ => Err(::capnp::Error::failed("Unsupported Id type".to_string())),
//         }
//     }
// }

// impl ToCapnp for File {
//     type Builder<'a> = expr_capnp::file::Builder<'a>;

//     fn to_capnp(&self, mut builder: Self::Builder<'_>) {
//         builder.set_bucket(self.bucket.as_str());
//         builder.set_key(self.key.as_str());
//     }
// }

// impl FromCapnp for File {
//     type Reader<'a> = expr_capnp::file::Reader<'a>;

//     fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
//         let bucket = reader.get_bucket()?.to_string()?;
//         let key = reader.get_key()?.to_string()?;
//         Ok(File { bucket, key })
//     }
// }

// impl ToCapnp for BTreeMap<String, Value> {
//     type Builder<'a> = expr_capnp::btree_value_map::Builder<'a>;

//     fn to_capnp(&self, builder: Self::Builder<'_>) {
//         let mut items_builder = builder.init_items(self.len() as u32);
//         for (index, (key, value)) in self.iter().enumerate() {
//             let mut entry_builder = items_builder.reborrow().get(index as u32);
//             entry_builder.set_key(key.as_str());
//             let value_builder = entry_builder.init_value();
//             value.to_capnp(value_builder);
//         }
//     }
// }

// impl FromCapnp for BTreeMap<String, Value> {
//     type Reader<'a> = expr_capnp::btree_value_map::Reader<'a>;

//     fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
//         let mut map = BTreeMap::new();
//         for entry in reader.get_items()? {
//             let key = entry.get_key()?.to_string()?;
//             let value = Value::from_capnp(entry.get_value()?)?;
//             map.insert(key, value);
//         }
//         Ok(map)
//     }
// }

// impl ToCapnp for Object {
//     type Builder<'a> = expr_capnp::object::Builder<'a>;

//     fn to_capnp(&self, builder: Self::Builder<'_>) {
//         let mut btree_map_builder = builder.init_map();
//         self.0.to_capnp(btree_map_builder);
//     }
// }

// impl FromCapnp for Object {
//     type Reader<'a> = expr_capnp::object::Reader<'a>;

//     fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
//         let btree_map_reader = reader.get_map()?;
//         let map = BTreeMap::from_capnp(btree_map_reader)?;
//         Ok(Object(map))
//     }
// }

// impl ToCapnp for Array {
//     type Builder<'a> = expr_capnp::array::Builder<'a>;

//     fn to_capnp(&self, builder: Self::Builder<'_>) {
//         let mut list_builder = builder.init_values(self.0.len() as u32);
//         for (index, value) in self.0.iter().enumerate() {
//             let item_builder = list_builder.reborrow().get(index as u32);
//             value.to_capnp(item_builder);
//         }
//     }
// }

// impl FromCapnp for Array {
//     type Reader<'a> = expr_capnp::array::Reader<'a>;

//     fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
//         let mut vec = Vec::new();

//         for item in reader.get_values()? {
//             vec.push(Value::from_capnp(item)?);
//         }
//         Ok(Array(vec))
//     }
// }

// impl ToCapnp for Geometry {
//     type Builder<'a> = expr_capnp::geometry::Builder<'a>;

//     fn to_capnp(&self, mut builder: Self::Builder<'_>) {
//         match self {
//             Geometry::Point(point) => {
//                 let point_builder = builder.init_point();
//                 point.to_capnp(point_builder);
//             }
//             Geometry::Line(line_string) => {
//                 let line_string_builder = builder.init_line();
//                 line_string.to_capnp(line_string_builder);
//             }
//             Geometry::Polygon(polygon) => {
//                 let polygon_builder = builder.init_polygon();
//                 polygon.to_capnp(polygon_builder);
//             }
//             Geometry::MultiPoint(multi_point) => {
//                 let multi_point_builder = builder.init_multi_point();
//                 multi_point.to_capnp(multi_point_builder);
//             }
//             Geometry::MultiLine(multi_line_string) => {
//                 let multi_line_string_builder = builder.init_multi_line();
//                 multi_line_string.to_capnp(multi_line_string_builder);
//             }
//             Geometry::MultiPolygon(multi_polygon) => {
//                 let multi_polygon_builder = builder.init_multi_polygon();
//                 multi_polygon.to_capnp(multi_polygon_builder);
//             }
//             Geometry::Collection(geometries) => {
//                 let geometry_collection_builder = builder.init_collection();
//                 let mut geometries_builder = geometry_collection_builder.init_geometries(geometries.len() as u32);
//                 for (index, geometry) in geometries.iter().enumerate() {
//                     let geometry_builder = geometries_builder.reborrow().get(index as u32);
//                     geometry.to_capnp(geometry_builder);
//                 }
//             }
//         }
//     }
// }

// impl FromCapnp for Geometry {
//     type Reader<'a> = expr_capnp::geometry::Reader<'a>;

//     fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
//         match reader.which()? {
//             expr_capnp::geometry::Which::Point(point) => Ok(Geometry::Point(geo::Point::from_capnp(point?)?)),
//             expr_capnp::geometry::Which::Line(line_string) => Ok(Geometry::Line(geo::LineString::from_capnp(line_string?)?)),
//             expr_capnp::geometry::Which::Polygon(polygon) => Ok(Geometry::Polygon(geo::Polygon::from_capnp(polygon?)?)),
//             expr_capnp::geometry::Which::MultiPoint(multi_point) => Ok(Geometry::MultiPoint(geo::MultiPoint::from_capnp(multi_point?)?)),
//             expr_capnp::geometry::Which::MultiLine(multi_line_string) => Ok(Geometry::MultiLine(geo::MultiLineString::from_capnp(multi_line_string?)?)),
//             expr_capnp::geometry::Which::MultiPolygon(multi_polygon) => Ok(Geometry::MultiPolygon(geo::MultiPolygon::from_capnp(multi_polygon?)?)),
//             expr_capnp::geometry::Which::Collection(geometry_collection) => {
//                 let geometry_collection = geometry_collection?;
//                 let geometries_reader = geometry_collection.get_geometries()?;
//                 let mut geometries = Vec::with_capacity(geometries_reader.len() as usize);
//                 for geometry in geometries_reader {
//                     geometries.push(Geometry::from_capnp(geometry)?);
//                 }
//                 Ok(Geometry::Collection(geometries))
//             }
//         }
//     }
// }

// impl ToCapnp for geo::Point {
//     type Builder<'a> = expr_capnp::geometry::point::Builder<'a>;

//     fn to_capnp(&self, mut builder: Self::Builder<'_>) {
//         builder.set_x(self.x());
//         builder.set_y(self.y());
//     }
// }

// impl FromCapnp for geo::Point {
//     type Reader<'a> = expr_capnp::geometry::point::Reader<'a>;

//     fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
//         let x = reader.get_x();
//         let y = reader.get_y();
//         Ok(Self::new(x, y))
//     }
// }

// impl ToCapnp for geo::Coord {
//     type Builder<'a> = expr_capnp::geometry::point::Builder<'a>;

//     fn to_capnp(&self, mut builder: Self::Builder<'_>) {
//         builder.set_x(self.x);
//         builder.set_y(self.y);
//     }
// }
// impl FromCapnp for geo::Coord {
//     type Reader<'a> = expr_capnp::geometry::point::Reader<'a>;

//     fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
//         let x = reader.get_x();
//         let y = reader.get_y();
//         Ok(Self { x, y })
//     }
// }

// impl ToCapnp for geo::LineString {
//     type Builder<'a> = expr_capnp::geometry::line_string::Builder<'a>;

//     fn to_capnp(&self, mut builder: Self::Builder<'_>) {
//         let mut points_builder = builder.init_points(self.0.len() as u32);
//         for (index, point) in self.0.iter().enumerate() {
//             let point_builder = points_builder.reborrow().get(index as u32);
//             point.to_capnp(point_builder);
//         }
//     }
// }

// impl FromCapnp for geo::LineString {
//     type Reader<'a> = expr_capnp::geometry::line_string::Reader<'a>;

//     fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
//         let mut points = Vec::new();
//         for point in reader.get_points()? {
//             points.push(geo::Coord::from_capnp(point)?);
//         }
//         Ok(Self(points))
//     }
// }

// impl ToCapnp for geo::Polygon {
//     type Builder<'a> = expr_capnp::geometry::polygon::Builder<'a>;

//     fn to_capnp(&self, mut builder: Self::Builder<'_>) {
//         let mut exterior_builder = builder.reborrow().init_exterior();
//         self.exterior().to_capnp(exterior_builder);

//         let interiors = self.interiors();
//         let mut interiors_builder = builder.reborrow().init_interiors(interiors.len() as u32);
//         for (index, interior) in interiors.iter().enumerate() {
//             let interior_builder = interiors_builder.reborrow().get(index as u32);
//             interior.to_capnp(interior_builder);
//         }
//     }
// }

// impl FromCapnp for geo::Polygon {
//     type Reader<'a> = expr_capnp::geometry::polygon::Reader<'a>;

//     fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
//         let exterior = reader.get_exterior()?;
//         let exterior = geo::LineString::from_capnp(exterior)?;
        
//         let mut interiors = Vec::new();
//         for interior in reader.get_interiors()? {
//             interiors.push(geo::LineString::from_capnp(interior)?);
//         }
        
//         Ok(Self::new(exterior, interiors))
//     }
// }


// impl ToCapnp for geo::MultiPoint {
//     type Builder<'a> = expr_capnp::geometry::multi_point::Builder<'a>;

//     fn to_capnp(&self, mut builder: Self::Builder<'_>) {
//         let mut points_builder = builder.init_points(self.0.len() as u32);
//         for (index, point) in self.0.iter().enumerate() {
//             let point_builder = points_builder.reborrow().get(index as u32);
//             point.to_capnp(point_builder);
//         }
//     }
// }
// impl FromCapnp for geo::MultiPoint {
//     type Reader<'a> = expr_capnp::geometry::multi_point::Reader<'a>;

//     fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
//         let mut points = Vec::new();
//         for point in reader.get_points()? {
//             points.push(geo::Point::from_capnp(point)?);
//         }
//         Ok(Self(points))
//     }
// }
// impl ToCapnp for geo::MultiLineString {
//     type Builder<'a> = expr_capnp::geometry::multi_line_string::Builder<'a>;

//     fn to_capnp(&self, mut builder: Self::Builder<'_>) {
//         let mut lines_builder = builder.init_lines(self.0.len() as u32);
//         for (index, line) in self.0.iter().enumerate() {
//             let line_builder = lines_builder.reborrow().get(index as u32);
//             line.to_capnp(line_builder);
//         }
//     }
// }
// impl FromCapnp for geo::MultiLineString {
//     type Reader<'a> = expr_capnp::geometry::multi_line_string::Reader<'a>;

//     fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
//         let mut lines = Vec::new();
//         for line in reader.get_lines()? {
//             lines.push(geo::LineString::from_capnp(line)?);
//         }
//         Ok(Self(lines))
//     }
// }
// impl ToCapnp for geo::MultiPolygon {
//     type Builder<'a> = expr_capnp::geometry::multi_polygon::Builder<'a>;

//     fn to_capnp(&self, mut builder: Self::Builder<'_>) {
//         let mut polygons_builder = builder.init_polygons(self.0.len() as u32);
//         for (index, polygon) in self.0.iter().enumerate() {
//             let polygon_builder = polygons_builder.reborrow().get(index as u32);
//             polygon.to_capnp(polygon_builder);
//         }
//     }
// }
// impl FromCapnp for geo::MultiPolygon {
//     type Reader<'a> = expr_capnp::geometry::multi_polygon::Reader<'a>;

//     fn from_capnp(reader: Self::Reader<'_>) -> ::capnp::Result<Self> {
//         let mut polygons = Vec::new();
//         for polygon in reader.get_polygons()? {
//             polygons.push(geo::Polygon::from_capnp(polygon)?);
//         }
//         Ok(Self(polygons))
//     }
// }

/// Flatbuffer conversions

impl ToFlatbuffers for Value {
    type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Value<'bldr>>;

    fn to_fb<'bldr>(
            &self,
            builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
        ) -> Self::Output<'bldr> {
        let args = match self {
            Self::Null => expr_fb::ValueArgs {
                value_type: expr_fb::ValueType::Null,
                value: Some(expr_fb::NullValue::create(builder, &expr_fb::NullValueArgs {}).as_union_value())
            },
            Self::Bool(b) => expr_fb::ValueArgs {
                value_type: expr_fb::ValueType::Bool,
                value: Some(expr_fb::BoolValue::create(builder, &expr_fb::BoolValueArgs { value: *b }).as_union_value())
            },
            Self::Number(n) => {
                match n {
                    crate::expr::Number::Int(i) => expr_fb::ValueArgs {
                        value_type: expr_fb::ValueType::Int64,
                        value: Some(expr_fb::Int64Value::create(builder, &expr_fb::Int64ValueArgs { value: *i }).as_union_value())
                    },
                    crate::expr::Number::Float(f) => expr_fb::ValueArgs {
                        value_type: expr_fb::ValueType::Float64,
                        value: Some(expr_fb::Float64Value::create(builder, &expr_fb::Float64ValueArgs { value: *f }).as_union_value())
                    },
                    crate::expr::Number::Decimal(d) => expr_fb::ValueArgs {
                        value_type: expr_fb::ValueType::Decimal,
                        value: Some(d.to_fb(builder).as_union_value())
                    },
                }
            },
            Self::Strand(s) => expr_fb::ValueArgs {
                value_type: expr_fb::ValueType::String,
                value: Some(s.to_fb(builder).as_union_value())
            },
            Self::Bytes(b) => {
                let bytes = builder.create_vector(b.as_slice());
                expr_fb::ValueArgs {
                    value_type: expr_fb::ValueType::Bytes,
                    value: Some(common_fb::Bytes::create(builder, &common_fb::BytesArgs { value: Some(bytes) }).as_union_value())
                }
        },
            Self::Thing(thing) => expr_fb::ValueArgs {
                value_type: expr_fb::ValueType::RecordId,
                value: Some(thing.to_fb(builder).as_union_value())
            },
            Self::Duration(d) => expr_fb::ValueArgs {
                value_type: expr_fb::ValueType::Duration,
                value: Some(d.to_fb(builder).as_union_value())
            },
            Self::Datetime(dt) => expr_fb::ValueArgs {
                value_type: expr_fb::ValueType::Timestamp,
                value: Some(dt.to_fb(builder).as_union_value())
            },
            Self::Uuid(uuid) => expr_fb::ValueArgs {
                value_type: expr_fb::ValueType::Uuid,
                value: Some(uuid.to_fb(builder).as_union_value())
            },
            Self::Object(obj) => expr_fb::ValueArgs {
                value_type: expr_fb::ValueType::Object,
                value: Some(obj.to_fb(builder).as_union_value())
            },
            Self::Array(arr) => expr_fb::ValueArgs {
                value_type: expr_fb::ValueType::Array,
                value: Some(arr.to_fb(builder).as_union_value())
            },
            Self::Geometry(geometry) => expr_fb::ValueArgs {
                value_type: expr_fb::ValueType::Geometry,
                value: Some(geometry.to_fb(builder).as_union_value())
            },
            Self::File(file) => expr_fb::ValueArgs {
                value_type: expr_fb::ValueType::File,
                value: Some(file.to_fb(builder).as_union_value())
            },
            _ => {
                // TODO: DO NOT PANIC, we just need to modify the Value enum which Mees is currently working on.
                panic!("Unsupported value type for Flatbuffers serialization: {:?}", self);
            }
        };

        expr_fb::Value::create(
            builder,
            &args,
        )
    }
}

impl FromFlatbuffers for Value {
    type Input<'a> = expr_fb::Value<'a>;

    fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
        match input.value_type() {
            expr_fb::ValueType::Null => Ok(Value::Null),
            expr_fb::ValueType::Bool => Ok(Value::Bool(input.value_as_bool().expect("Guaranteed to be a Bool").value())),
            expr_fb::ValueType::Int64 => Ok(Value::Number(Number::Int(input.value_as_int_64().expect("Guaranteed to be an Int64").value()))),
            expr_fb::ValueType::Float64 => Ok(Value::Number(Number::Float(input.value_as_float_64().expect("Guaranteed to be a Float64").value()))),
            expr_fb::ValueType::Decimal => {
                let decimal_value = input.value_as_decimal().expect("Guaranteed to be a Decimal");
                let decimal = decimal_value.value().expect("Decimal value is guaranteed to be present").parse::<Decimal>()
                    .map_err(|_| anyhow!("Invalid decimal format"))?;
                Ok(Value::Number(Number::Decimal(decimal)))
            }
            expr_fb::ValueType::String => {
                let string_value = input.value_as_string().expect("Guaranteed to be a String");
                let value = string_value.value().expect("String value is guaranteed to be present").to_string();
                Ok(Value::Strand(Strand(value)))
            }
            expr_fb::ValueType::Bytes => {
                let bytes_value = input.value_as_bytes().expect("Guaranteed to be Bytes");
                let value = Vec::<u8>::from_fb(bytes_value.value().expect("Bytes value is guaranteed to be present"))?;
                Ok(Value::Bytes(crate::expr::Bytes(value)))
            }
            expr_fb::ValueType::RecordId => {
                let record_id_value = input.value_as_record_id().expect("Guaranteed to be a RecordId");
                let thing = Thing::from_fb(record_id_value)?;
                Ok(Value::Thing(thing))
            }
            expr_fb::ValueType::Duration => {
                let duration_value = input.value_as_duration().expect("Guaranteed to be a Duration");
                let duration = Duration::from_fb(duration_value)?;
                Ok(Value::Duration(duration))
            }
            expr_fb::ValueType::Timestamp => {
                let timestamp_value = input.value_as_timestamp().expect("Guaranteed to be a Timestamp");
                let dt = DateTime::<Utc>::from_fb(timestamp_value)?;
                Ok(Value::Datetime(Datetime(dt)))
            }
            expr_fb::ValueType::Uuid => {
                let uuid_value = input.value_as_uuid().expect("Guaranteed to be a Uuid");
                let uuid = Uuid::from_fb(uuid_value)?;
                Ok(Value::Uuid(uuid))
            }
            expr_fb::ValueType::Object => {
                let object_value = input.value_as_object().expect("Guaranteed to be an Object");
                let object = Object::from_fb(object_value)?;
                Ok(Value::Object(object))
            }
            expr_fb::ValueType::Array => {
                let array_value = input.value_as_array().expect("Guaranteed to be an Array");
                let array = Array::from_fb(array_value)?;
                Ok(Value::Array(array))
            }
            expr_fb::ValueType::Geometry => {
                let geometry_value = input.value_as_geometry().expect("Guaranteed to be a Geometry");
                let geometry = Geometry::from_fb(geometry_value)?;
                Ok(Value::Geometry(geometry))
            }
            expr_fb::ValueType::File => {
                let file_value = input.value_as_file().expect("Guaranteed to be a File");
                let file = File::from_fb(file_value)?;
                Ok(Value::File(file))
            }
            _ => Err(anyhow!("Unsupported value type for Flatbuffers deserialization: {:?}", input.value_type())),
        }
    }
}


impl ToFlatbuffers for i64 {
    type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Int64Value<'bldr>>;

    fn to_fb<'bldr>(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> Self::Output<'bldr> {
        expr_fb::Int64Value::create(builder, &expr_fb::Int64ValueArgs {
            value: *self,
        })
    }
}

impl FromFlatbuffers for i64 {
    type Input<'a> = expr_fb::Int64Value<'a>;

    fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
        Ok(input.value())
    }
}

impl ToFlatbuffers for f64 {
    type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Float64Value<'bldr>>;

    fn to_fb<'bldr>(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> Self::Output<'bldr> {
        expr_fb::Float64Value::create(builder, &expr_fb::Float64ValueArgs {
            value: *self,
        })
    }
}

impl FromFlatbuffers for f64 {
    type Input<'a> = expr_fb::Float64Value<'a>;

    fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
        Ok(input.value())
    }
}

impl ToFlatbuffers for String {
    type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::StringValue<'bldr>>;

    fn to_fb<'bldr>(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> Self::Output<'bldr> {
        let value = builder.create_string(self);
        expr_fb::StringValue::create(builder, &expr_fb::StringValueArgs {
            value: Some(value),
        })
    }
}

impl ToFlatbuffers for Decimal {
    type Output<'bldr> = flatbuffers::WIPOffset<common_fb::Decimal<'bldr>>;

    fn to_fb<'bldr>(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> Self::Output<'bldr> {
        let value = builder.create_string(&self.to_string());
        common_fb::Decimal::create(builder, &common_fb::DecimalArgs {
            value: Some(value),
        })
    }
}

impl ToFlatbuffers for Duration {
    type Output<'bldr> = flatbuffers::WIPOffset<common_fb::Duration<'bldr>>;

    fn to_fb<'bldr>(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> Self::Output<'bldr> {
        common_fb::Duration::create(builder, &common_fb::DurationArgs {
            seconds: self.as_secs(),
            nanos: self.subsec_nanos(),
        })
    }
}

impl FromFlatbuffers for Duration {
    type Input<'a> = common_fb::Duration<'a>;

    fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
        let seconds = input.seconds();
        let nanos = input.nanos() as u32;
        Ok(Duration::new(seconds, nanos))
    }
}

impl ToFlatbuffers for DateTime<Utc> {
    type Output<'bldr> = flatbuffers::WIPOffset<common_fb::Timestamp<'bldr>>;

    fn to_fb<'bldr>(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> Self::Output<'bldr> {
        common_fb::Timestamp::create(builder, &common_fb::TimestampArgs {
            seconds: self.timestamp(),
            nanos: self.timestamp_subsec_nanos(),
        })
    }
}

impl FromFlatbuffers for DateTime<Utc> {
    type Input<'a> = common_fb::Timestamp<'a>;

    fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
        let seconds = input.seconds();
        let nanos = input.nanos() as u32;
        DateTime::<Utc>::from_timestamp(seconds, nanos)
            .ok_or_else(|| anyhow::anyhow!("Invalid timestamp format"))
    }
}

impl ToFlatbuffers for Uuid {
    type Output<'bldr> = flatbuffers::WIPOffset<common_fb::Uuid<'bldr>>;

    fn to_fb<'bldr>(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> Self::Output<'bldr> {
        let bytes = builder.create_vector(self.as_bytes());
        common_fb::Uuid::create(builder, &common_fb::UuidArgs {
            bytes: Some(bytes),
        })
    }
}

impl FromFlatbuffers for Uuid {
    type Input<'a> = common_fb::Uuid<'a>;

    fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
        let bytes_vector = input.bytes().ok_or_else(|| anyhow::anyhow!("Missing bytes in Uuid"))?;
        Uuid::from_slice(bytes_vector.bytes()).map_err(|_| anyhow::anyhow!("Invalid UUID format"))
    }
}

impl ToFlatbuffers for Thing {
    type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::RecordId<'bldr>>;

    fn to_fb<'bldr>(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> Self::Output<'bldr> {
        let table = builder.create_string(&self.tb);
        let id = self.id.to_fb(builder);
        expr_fb::RecordId::create(builder, &expr_fb::RecordIdArgs {
            table: Some(table),
            id: Some(id),
        })
    }
}

impl FromFlatbuffers for Thing {
    type Input<'a> = expr_fb::RecordId<'a>;

    fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
        let table = input.table().ok_or_else(|| anyhow::anyhow!("Missing table in RecordId"))?;
        let id = Id::from_fb(input.id().ok_or_else(|| anyhow::anyhow!("Missing id in RecordId"))?)?;
        Ok(Thing {
            tb: table.to_string(),
            id,
        })
    }
}

impl FromFlatbuffers for Vec<u8> {
    type Input<'a> = flatbuffers::Vector<'a, u8>;

    fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
        Ok(input.bytes().to_vec())
    }
}

impl ToFlatbuffers for Id {
    type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Id<'bldr>>;

    fn to_fb<'bldr>(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> Self::Output<'bldr> {
        match self {
            Id::Number(n) => {
                let id = n.to_fb(builder).as_union_value();
                expr_fb::Id::create(builder, &expr_fb::IdArgs {
                id_type: expr_fb::IdType::Int64,
                id: Some(id),
            })},
            Id::String(s) => {
                let id = s.to_fb(builder).as_union_value();
                expr_fb::Id::create(builder, &expr_fb::IdArgs {
                id_type: expr_fb::IdType::String,
                id: Some(id),
            })},
            Id::Uuid(uuid) => {
                let id = uuid.to_fb(builder).as_union_value();
                expr_fb::Id::create(builder, &expr_fb::IdArgs {
                id_type: expr_fb::IdType::Uuid,
                id: Some(id),
            })},
            Id::Array(arr) => {
                let id = arr.to_fb(builder).as_union_value();
                expr_fb::Id::create(builder, &expr_fb::IdArgs {
                id_type: expr_fb::IdType::Array,
                id: Some(id),
            })},
            _ => panic!("Unsupported Id type for FlatBuffers serialization: {:?}", self),
        }
    }
}

impl FromFlatbuffers for Id {
    type Input<'a> = expr_fb::Id<'a>;

    fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
        match input.id_type() {
            expr_fb::IdType::Int64 => {
                let id_value = input.id_as_int_64().ok_or_else(|| anyhow::anyhow!("Expected Int64 Id"))?;
                Ok(Id::Number(id_value.value()))
            }
            expr_fb::IdType::String => {
                let id_value = input.id_as_string().ok_or_else(|| anyhow::anyhow!("Expected String Id"))?;
                Ok(Id::String(id_value.value().ok_or_else(|| anyhow::anyhow!("Missing String value"))?.to_string()))
            }
            expr_fb::IdType::Uuid => {
                let id_value = input.id_as_uuid().ok_or_else(|| anyhow::anyhow!("Expected Uuid Id"))?;
                let uuid = Uuid::from_fb(id_value)?;
                Ok(Id::Uuid(uuid))
            }
            expr_fb::IdType::Array => {
                let id_value = input.id_as_array().ok_or_else(|| anyhow::anyhow!("Expected Array Id"))?;
                let array = Array::from_fb(id_value)?;
                Ok(Id::Array(array))
            }
            _ => Err(anyhow::anyhow!("Unsupported Id type for FlatBuffers deserialization: {:?}", input.id_type())),
        }
    }
}


impl ToFlatbuffers for File {
    type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::File<'bldr>>;

    fn to_fb<'bldr>(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> Self::Output<'bldr> {
        let bucket = builder.create_string(&self.bucket);
        let key = builder.create_string(&self.key);
        expr_fb::File::create(builder, &expr_fb::FileArgs {
            bucket: Some(bucket),
            key: Some(key),
        })
    }
}

impl FromFlatbuffers for File {
    type Input<'a> = expr_fb::File<'a>;

    fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
        let bucket = input.bucket().ok_or_else(|| anyhow::anyhow!("Missing bucket in File"))?;
        let key = input.key().ok_or_else(|| anyhow::anyhow!("Missing key in File"))?;
        Ok(File {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })
    }
}

impl ToFlatbuffers for Object {
    type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Object<'bldr>>;

    fn to_fb<'bldr>(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> Self::Output<'bldr> {
        let mut entries = Vec::with_capacity(self.0.len());
        for (key, value) in &self.0 {
            let key_fb = builder.create_string(key);
            let value_fb = value.to_fb(builder);

            let object_item = expr_fb::KeyValue::create(builder, &&expr_fb::KeyValueArgs {
                key: Some(key_fb),
                value: Some(value_fb),
            });

            entries.push(object_item);
        }
        let entries_vector = builder.create_vector(&entries);
        expr_fb::Object::create(builder, &expr_fb::ObjectArgs {
            items: Some(entries_vector),
        })
    }
}

impl FromFlatbuffers for Object {
    type Input<'a> = expr_fb::Object<'a>;

    fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
        let mut map = BTreeMap::new();
        let items = input.items().ok_or_else(|| anyhow::anyhow!("Missing items in Object"))?;
        if items.is_empty() {
            return Ok(Object(map));
        }
        for entry in items {
            let key = entry.key().context("Missing key in Object entry")?.to_string();
            let value = entry.value().context("Missing value in Object entry")?;
            map.insert(key, Value::from_fb(value)?);
        }
        Ok(Object(map))
    }
}

impl ToFlatbuffers for Array {
    type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Array<'bldr>>;

    fn to_fb<'bldr>(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> Self::Output<'bldr> {
        let mut values = Vec::with_capacity(self.0.len());
        for value in &self.0 {
            values.push(value.to_fb(builder));
        }
        let values_vector = builder.create_vector(&values);
        expr_fb::Array::create(builder, &expr_fb::ArrayArgs { values: Some(values_vector) })
    }
}

impl FromFlatbuffers for Array {
    type Input<'a> = expr_fb::Array<'a>;

    fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
        let mut vec = Vec::new();
        let values = input.values().context("Values is not set")?;
        for value in values {
            vec.push(Value::from_fb(value)?);
        }
        Ok(Array(vec))
    }
}

impl ToFlatbuffers for Geometry {
    type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Geometry<'bldr>>;

    fn to_fb<'bldr>(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> Self::Output<'bldr> {
        match self {
            Geometry::Point(point) => {
                let geometry = point.to_fb(builder);
                expr_fb::Geometry::create(builder, &expr_fb::GeometryArgs {
                geometry_type: expr_fb::GeometryType::Point,
                geometry: Some(geometry.as_union_value()),
            })},
            Geometry::Line(line_string) => {
                let geometry = line_string.to_fb(builder);
                expr_fb::Geometry::create(builder, &expr_fb::GeometryArgs {
                    geometry_type: expr_fb::GeometryType::LineString,
                    geometry: Some(geometry.as_union_value()),
                })
            }
            Geometry::Polygon(polygon) => {
                let geometry = polygon.to_fb(builder);
                expr_fb::Geometry::create(builder, &expr_fb::GeometryArgs {
                    geometry_type: expr_fb::GeometryType::Polygon,
                    geometry: Some(geometry.as_union_value()),
                })
            }
            Geometry::MultiPoint(multi_point) => {
                let geometry = multi_point.to_fb(builder);
                expr_fb::Geometry::create(builder, &expr_fb::GeometryArgs {
                    geometry_type: expr_fb::GeometryType::MultiPoint,
                    geometry: Some(geometry.as_union_value()),
                })
            }
            Geometry::MultiLine(multi_line_string) => {
                let geometry = multi_line_string.to_fb(builder);
                expr_fb::Geometry::create(builder, &expr_fb::GeometryArgs {
                    geometry_type: expr_fb::GeometryType::MultiLineString,
                    geometry: Some(geometry.as_union_value()),
                })
            }
            Geometry::MultiPolygon(multi_polygon) => {
                let geometry = multi_polygon.to_fb(builder);
                expr_fb::Geometry::create(builder, &expr_fb::GeometryArgs {
                    geometry_type: expr_fb::GeometryType::MultiPolygon,
                    geometry: Some(geometry.as_union_value()),
                })
            }
            Geometry::Collection(geometries) => {
                let mut geometries_vec = Vec::with_capacity(geometries.len());
                for geometry in geometries {
                    geometries_vec.push(geometry.to_fb(builder));
                }
                let geometries_vector = builder.create_vector(&geometries_vec);

                let collection = expr_fb::GeometryCollection::create(builder, &expr_fb::GeometryCollectionArgs {
                    geometries: Some(geometries_vector),
                });

                expr_fb::Geometry::create(builder, &expr_fb::GeometryArgs {
                    geometry_type: expr_fb::GeometryType::Collection,
                    geometry: Some(collection.as_union_value()),
                })
            }
        }
    }
}

impl FromFlatbuffers for Geometry {
    type Input<'a> = expr_fb::Geometry<'a>;

    fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
        match input.geometry_type() {
            expr_fb::GeometryType::Point => {
                let point = input.geometry_as_point().ok_or_else(|| anyhow::anyhow!("Expected Point geometry"))?;
                Ok(Geometry::Point(geo::Point::from_fb(point)?))
            }
            expr_fb::GeometryType::LineString => {
                let line_string = input.geometry_as_line_string().ok_or_else(|| anyhow::anyhow!("Expected LineString geometry"))?;
                Ok(Geometry::Line(geo::LineString::from_fb(line_string)?))
            }
            expr_fb::GeometryType::Polygon => {
                let polygon = input.geometry_as_polygon().ok_or_else(|| anyhow::anyhow!("Expected Polygon geometry"))?;
                Ok(Geometry::Polygon(geo::Polygon::from_fb(polygon)?))
            }
            expr_fb::GeometryType::MultiPoint => {
                let multi_point = input.geometry_as_multi_point().ok_or_else(|| anyhow::anyhow!("Expected MultiPoint geometry"))?;
                Ok(Geometry::MultiPoint(geo::MultiPoint::from_fb(multi_point)?))
            }
            expr_fb::GeometryType::MultiLineString => {
                let multi_line_string = input.geometry_as_multi_line_string().ok_or_else(|| anyhow::anyhow!("Expected MultiLineString geometry"))?;
                Ok(Geometry::MultiLine(geo::MultiLineString::from_fb(multi_line_string)?))
            }
            expr_fb::GeometryType::MultiPolygon => {
                let multi_polygon = input.geometry_as_multi_polygon().ok_or_else(|| anyhow::anyhow!("Expected MultiPolygon geometry"))?;
                Ok(Geometry::MultiPolygon(geo::MultiPolygon::from_fb(multi_polygon)?))
            }
            expr_fb::GeometryType::Collection => {
                let collection = input.geometry_as_collection().ok_or_else(|| anyhow::anyhow!("Expected GeometryCollection"))?;
                let geometries_reader = collection.geometries().context("Geometries is not set")?;
                let mut geometries = Vec::with_capacity(geometries_reader.len() as usize);
                for geometry in geometries_reader {
                    geometries.push(Geometry::from_fb(geometry)?);
                }
                Ok(Geometry::Collection(geometries))
            }
            _ => Err(anyhow::anyhow!("Unsupported geometry type for FlatBuffers deserialization: {:?}", input.geometry_type())),
        }
    }
}

impl ToFlatbuffers for geo::Point {
    type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Point<'bldr>>;

    fn to_fb<'bldr>(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> Self::Output<'bldr> {
        expr_fb::Point::create(builder, &expr_fb::PointArgs {
            x: self.x(),
            y: self.y(),
        })
    }
}

impl FromFlatbuffers for geo::Point {
    type Input<'a> = expr_fb::Point<'a>;

    fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
        Ok(geo::Point::new(input.x(), input.y()))
    }
}

impl ToFlatbuffers for geo::Coord {
    type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Point<'bldr>>;

    fn to_fb<'bldr>(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> Self::Output<'bldr> {
        expr_fb::Point::create(builder, &expr_fb::PointArgs {
            x: self.x,
            y: self.y,
        })
    }
}

impl FromFlatbuffers for geo::Coord {
    type Input<'a> = expr_fb::Point<'a>;

    fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
        Ok(geo::Coord {
            x: input.x(),
            y: input.y(),
        })
    }
}

impl ToFlatbuffers for geo::LineString {
    type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::LineString<'bldr>>;

    fn to_fb<'bldr>(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> Self::Output<'bldr> {
        let mut points = Vec::with_capacity(self.0.len());
        for point in &self.0 {
            points.push(point.to_fb(builder));
        }
        let points_vector = builder.create_vector(&points);
        expr_fb::LineString::create(builder, &expr_fb::LineStringArgs {
            points: Some(points_vector),
        })
    }
}

impl FromFlatbuffers for geo::LineString {
    type Input<'a> = expr_fb::LineString<'a>;

    fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
        let mut points = Vec::new();
        for point in input.points().context("Points is not set")? {
            points.push(geo::Coord::from_fb(point)?);
        }
        Ok(Self(points))
    }
}

impl ToFlatbuffers for geo::Polygon {
    type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::Polygon<'bldr>>;

    fn to_fb<'bldr>(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> Self::Output<'bldr> {
        let exterior = self.exterior().to_fb(builder);
        let mut interiors = Vec::with_capacity(self.interiors().len());
        for interior in self.interiors() {
            interiors.push(interior.to_fb(builder));
        }
        let interiors_vector = builder.create_vector(&interiors);
        expr_fb::Polygon::create(builder, &expr_fb::PolygonArgs {
            exterior: Some(exterior),
            interiors: Some(interiors_vector),
        })
    }
}

impl FromFlatbuffers for geo::Polygon {
    type Input<'a> = expr_fb::Polygon<'a>;

    fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
        let exterior = input.exterior().ok_or_else(|| anyhow::anyhow!("Missing exterior in Polygon"))?;
        let exterior = geo::LineString::from_fb(exterior)?;
        
        let mut interiors = Vec::new();
        if let Some(interiors_reader) = input.interiors() {
            for interior in interiors_reader {
                interiors.push(geo::LineString::from_fb(interior)?);
            }
        }
        
        Ok(Self::new(exterior, interiors))
    }
}

impl ToFlatbuffers for geo::MultiPoint {
    type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::MultiPoint<'bldr>>;

    fn to_fb<'bldr>(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> Self::Output<'bldr> {
        let mut points = Vec::with_capacity(self.0.len());
        for point in &self.0 {
            points.push(point.to_fb(builder));
        }
        let points_vector = builder.create_vector(&points);
        expr_fb::MultiPoint::create(builder, &expr_fb::MultiPointArgs {
            points: Some(points_vector),
        })
    }
}

impl FromFlatbuffers for geo::MultiPoint {
    type Input<'a> = expr_fb::MultiPoint<'a>;

    fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
        let mut points = Vec::new();
        for point in input.points().context("Points is not set")? {
            points.push(geo::Point::from_fb(point)?);
        }
        Ok(Self(points))
    }
}

impl ToFlatbuffers for geo::MultiLineString {
    type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::MultiLineString<'bldr>>;

    fn to_fb<'bldr>(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> Self::Output<'bldr> {
        let mut lines = Vec::with_capacity(self.0.len());
        for line in &self.0 {
            lines.push(line.to_fb(builder));
        }
        let lines_vector = builder.create_vector(&lines);
        expr_fb::MultiLineString::create(builder, &expr_fb::MultiLineStringArgs {
            lines: Some(lines_vector),
        })
    }
}

impl FromFlatbuffers for geo::MultiLineString {
    type Input<'a> = expr_fb::MultiLineString<'a>;

    fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
        let mut lines = Vec::new();
        for line in input.lines().context("Lines is not set")? {
            lines.push(geo::LineString::from_fb(line)?);
        }
        Ok(Self(lines))
    }
}

impl ToFlatbuffers for geo::MultiPolygon {
    type Output<'bldr> = flatbuffers::WIPOffset<expr_fb::MultiPolygon<'bldr>>;

    fn to_fb<'bldr>(
        &self,
        builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
    ) -> Self::Output<'bldr> {
        let mut polygons = Vec::with_capacity(self.0.len());
        for polygon in &self.0 {
            polygons.push(polygon.to_fb(builder));
        }
        let polygons_vector = builder.create_vector(&polygons);
        expr_fb::MultiPolygon::create(builder, &expr_fb::MultiPolygonArgs {
            polygons: Some(polygons_vector),
        })
    }
}

impl FromFlatbuffers for geo::MultiPolygon {
    type Input<'a> = expr_fb::MultiPolygon<'a>;

    fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
        let mut polygons = Vec::new();
        for polygon in input.polygons().context("Polygons is not set")? {
            polygons.push(geo::Polygon::from_fb(polygon)?);
        }
        Ok(Self(polygons))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case::null(Value::Null)]
    #[case::bool(Value::Bool(true))]
    #[case::bool(Value::Bool(false))]
    #[case::int(Value::Number(Number::Int(42)))]
    #[case::int(Value::Number(Number::Int(i64::MIN)))]
    #[case::int(Value::Number(Number::Int(i64::MAX)))]
    #[case::float(Value::Number(Number::Float(1.23)))]
    #[case::float(Value::Number(Number::Float(f64::MIN)))]
    #[case::float(Value::Number(Number::Float(f64::MAX)))]
    #[case::float(Value::Number(Number::Float(f64::NAN)))]
    #[case::float(Value::Number(Number::Float(f64::INFINITY)))]
    #[case::float(Value::Number(Number::Float(f64::NEG_INFINITY)))]
    #[case::decimal(Value::Number(Number::Decimal(Decimal::new(123, 2))))]
    #[case::duration(Value::Duration(Duration::new(1, 0)))]
    #[case::datetime(Value::Datetime(Datetime(DateTime::<Utc>::from_timestamp(1_000_000_000, 0).unwrap())))]
    #[case::uuid(Value::Uuid(Uuid::new_v4()))]
    #[case::string(Value::Strand(Strand("Hello, World!".to_string())))]
    #[case::bytes(Value::Bytes(crate::expr::Bytes(vec![1, 2, 3, 4, 5])))]
    #[case::thing(Value::Thing(Thing { tb: "test_table".to_string(), id: Id::Number(42) }))] // Example Thing
    #[case::object(Value::Object(Object(BTreeMap::from([("key".to_string(), Value::Strand(Strand("value".to_string())))]))))]
    #[case::array(Value::Array(Array(vec![Value::Number(Number::Int(1)), Value::Number(Number::Float(2.0))])))]
    #[case::geometry::point(Value::Geometry(Geometry::Point(geo::Point::new(1.0, 2.0))))]
    #[case::geometry::line(Value::Geometry(Geometry::Line(geo::LineString(vec![geo::Coord { x: 1.0, y: 2.0 }, geo::Coord { x: 3.0, y: 4.0 }]))))]
    #[case::geometry::polygon(Value::Geometry(Geometry::Polygon(geo::Polygon::new(
        geo::LineString(vec![geo::Coord { x: 0.0, y: 0.0 }, geo::Coord { x: 1.0, y: 1.0 }, geo::Coord { x: 0.0, y: 1.0 }]),
        vec![geo::LineString(vec![geo::Coord { x: 0.5, y: 0.5 }, geo::Coord { x: 0.75, y: 0.75 }])]
    ))))]
    #[case::geometry::multipoint(Value::Geometry(Geometry::MultiPoint(geo::MultiPoint(vec![geo::Point::new(1.0, 2.0), geo::Point::new(3.0, 4.0)]))))]
    #[case::geometry::multiline(Value::Geometry(Geometry::MultiLine(geo::MultiLineString(vec![geo::LineString(vec![geo::Coord { x: 1.0, y: 2.0 }, geo::Coord { x: 3.0, y: 4.0 }])] ))))]
    #[case::geometry::multipolygon(Value::Geometry(Geometry::MultiPolygon(geo::MultiPolygon(vec![geo::Polygon::new(
        geo::LineString(vec![geo::Coord { x: 0.0, y: 0.0 }, geo::Coord { x: 1.0, y: 1.0 }, geo::Coord { x: 0.0, y: 1.0 }]),
        vec![geo::LineString(vec![geo::Coord { x: 0.5, y: 0.5 }, geo::Coord { x: 0.75, y: 0.75 }])]
    )]))))]
    #[case::file(Value::File(File { bucket: "test_bucket".to_string(), key: "test_key".to_string() }))]
    fn test_flatbuffers_roundtrip(#[case] input: Value) {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let input_fb = input.to_fb(&mut builder);
        builder.finish_minimal(input_fb);
        let buf = builder.finished_data();
        let value_fb = flatbuffers::root::<expr_fb::Value>(buf).expect("Failed to read FlatBuffer");
        let value = Value::from_fb(value_fb).expect("Failed to convert from FlatBuffer");
        assert_eq!(input, value, "Roundtrip conversion failed for input: {:?}", input);
    }
}