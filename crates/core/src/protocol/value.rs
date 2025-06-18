// use std::collections::BTreeMap;

// use crate::protocol::surrealdb::value::{
//     Value as ValueProto,
//     Array as ArrayProto,
//     Object as ObjectProto,
//     Geometry as GeometryProto,
//     File as FileProto,
//     RecordId as RecordIdProto,
//     Id as IdProto,
//     Point as PointProto,
//     LineString as LineStringProto,
//     Polygon as PolygonProto,
//     MultiPoint as MultiPointProto,
//     MultiLineString as MultiLineStringProto,
//     MultiPolygon as MultiPolygonProto,
//     GeometryCollection as GeometryCollectionProto,
// };

// impl ValueProto {
//     pub fn downcast_str(&self) -> Option<&str> {
//         let Some(inner) = &self.inner else {
//             return None;
//         };
//         if let crate::protocol::surrealdb::value::value::Inner::Strand(s) = inner {
//             Some(s.as_str())
//         } else {
//             None
//         }
//     }
// }

// impl ObjectProto {
//     pub fn get(&self, key: &str) -> Option<&ValueProto> {
//         self.values.get(key)
//     }
// }

// impl TryFrom<ValueProto> for crate::expr::Value {
// 	type Error = anyhow::Error;

// 	fn try_from(proto: ValueProto) -> Result<Self, Self::Error> {
//         use crate::protocol::surrealdb::value::value;

// 		let Some(inner) = proto.inner else {
// 			return Ok(crate::expr::Value::None);
// 		};

// 		let value = match inner {
// 			value::Inner::Null(_) => crate::expr::Value::Null,
// 			value::Inner::Bool(v) => crate::expr::Value::Bool(v),
//             value::Inner::Int(v) => crate::expr::Value::Number(v.into()),
//             value::Inner::Float(v) => crate::expr::Value::Number(v.into()),
// 			value::Inner::Decimal(v) => crate::expr::Value::Number(
//                 crate::expr::Number::Decimal(crate::sql::DecimalExt::from_str_normalized(&v)?)
//             ),
// 			value::Inner::Strand(v) => crate::expr::Value::Strand(v.into()),
// 			value::Inner::Duration(v) => crate::expr::Value::Duration(v.into()),
// 			value::Inner::Datetime(v) => crate::expr::Value::Datetime(v.try_into()?),
// 			value::Inner::Uuid(v) => crate::expr::Value::Uuid(v.try_into()?),
// 			value::Inner::Array(v) => {
// 			    crate::expr::Value::Array(v.try_into()?)
// 			}
// 			value::Inner::Object(v) => {
// 			    crate::expr::Value::Object(v.try_into()?)
// 			}
// 			value::Inner::Geometry(v) => crate::expr::Value::Geometry(v.try_into()?),
// 			value::Inner::Bytes(v) => crate::expr::Value::Bytes(v.into()),
// 			value::Inner::RecordId(v) => crate::expr::Value::Thing(v.try_into()?),
// 			value::Inner::File(v) => crate::expr::Value::File(v.try_into()?),
// 		};

// 		Ok(value)
// 	}
// }

// impl TryFrom<crate::expr::Value> for ValueProto {
//     type Error = anyhow::Error;

// 	fn try_from(value: crate::expr::Value) -> Result<Self, Self::Error> {
//         use crate::expr::{Value, Number};
// 		use crate::protocol::surrealdb::value as value_proto;
// 		use crate::protocol::surrealdb::value::value::Inner as ValueInner;

// 		let inner = match value {
// 			// These value types are simple values which
// 			// can be used in query responses sent to
// 			// the client.
// 			Value::None => return Ok(Self {
// 				inner: None,
// 			}),
// 			Value::Null => ValueInner::Null(value_proto::ValueType::Null as i32),
// 			Value::Bool(boolean) => ValueInner::Bool(boolean),
// 			Value::Number(number) => match number {
// 				Number::Int(int) => ValueInner::Int(int),
// 				Number::Float(float) => ValueInner::Float(float),
// 				Number::Decimal(decimal) => ValueInner::Decimal(decimal.to_string()),
// 			},
// 			Value::Strand(strand) => ValueInner::Strand(strand.into()),
// 			Value::Duration(duration) => ValueInner::Duration(duration.into()),
// 			Value::Datetime(datetime) => ValueInner::Datetime(datetime.try_into()?),
// 			Value::Uuid(uuid) => ValueInner::Uuid(uuid.to_string()),
// 			Value::Array(array) => ValueInner::Array(array.try_into()?),
//             Value::Object(object) => ValueInner::Object(object.try_into()?),
//             Value::Geometry(geometry) => ValueInner::Geometry(geometry.try_into()?),
//             Value::Bytes(bytes) => ValueInner::Bytes(bytes.into()),
//             Value::Thing(thing) => ValueInner::RecordId(RecordIdProto {
//                 id: Some(thing.id.try_into()?),
//                 table: thing.tb,
//             }),
//             Value::File(file) => ValueInner::File(FileProto {
//                 bucket: file.bucket,
//                 key: file.key,
//             }),
//             Value::Idiom(_)
//             | Value::Param(_)
//             | Value::Function(_)
//             | Value::Table(_)
//             | Value::Mock(_)
//             | Value::Regex(_)
//             | Value::Cast(_)
//             | Value::Block(_)
//             | Value::Range(_)
//             | Value::Edges(_)
//             | Value::Future(_)
//             | Value::Constant(_)
//             | Value:: Subquery(_)
//             | Value::Expression(_)
//             | Value::Query(_)
//             | Value::Model(_)
//             | Value::Closure(_)
//             | Value::Refs(_) => {
//                 return Err(anyhow::anyhow!("Value is not network compatible: {:?}", value));
//             }
// 		};

// 		Ok(Self {
// 			inner: Some(inner)
// 		})
// 	}
// }

// impl From<String> for ValueProto {
//     fn from(value: String) -> Self {
//         ValueProto {
//             inner: Some(crate::protocol::surrealdb::value::value::Inner::Strand(value)),
//         }
//     }
// }
// impl From<&str> for ValueProto {
//     fn from(value: &str) -> Self {
//         ValueProto {
//             inner: Some(crate::protocol::surrealdb::value::value::Inner::Strand(value.to_string())),
//         }
//     }
// }

// impl From<super::google::protobuf::Duration> for crate::expr::Duration {
// 	fn from(proto: super::google::protobuf::Duration) -> Self {
// 		crate::expr::Duration(std::time::Duration::from_nanos(
// 			proto.seconds as u64 * 1_000_000_000 + proto.nanos as u64,
// 		))
// 	}
// }

// impl From<crate::expr::Duration> for super::google::protobuf::Duration {
//     fn from(duration: crate::expr::Duration) -> Self {
//         let nanos = duration.0.as_nanos() as u64;
//         Self {
//             seconds: (nanos / 1_000_000_000) as i64,
//             nanos: (nanos % 1_000_000_000) as i32,
//         }
//     }
// }

// impl TryFrom<super::google::protobuf::Timestamp> for crate::expr::Datetime {
// 	type Error = anyhow::Error;
// 	fn try_from(proto: super::google::protobuf::Timestamp) -> Result<Self, Self::Error> {
// 		Ok(crate::expr::Datetime(proto.try_into()?))
// 	}
// }

// impl TryFrom<crate::expr::Datetime> for super::google::protobuf::Timestamp {
//     type Error = anyhow::Error;

//     fn try_from(datetime: crate::expr::Datetime) -> Result<Self, Self::Error> {
//         let dt = datetime.0;
//         let seconds = dt.timestamp();
//         let nanos = dt.timestamp_subsec_nanos() as i32;
//         Ok(Self {
//             seconds,
//             nanos,
//         })
//     }
// }

// impl TryFrom<ArrayProto> for crate::expr::Array {
//     type Error = anyhow::Error;

//     fn try_from(proto: ArrayProto) -> Result<Self, Self::Error> {
//         let mut items = Vec::with_capacity(proto.values.len());
//         for item in proto.values {
//             items.push(crate::expr::Value::try_from(item)?);
//         }
//         Ok(crate::expr::Array(items))
//     }
// }

// impl TryFrom<crate::expr::Array> for ArrayProto {
//     type Error = anyhow::Error;

//     fn try_from(array: crate::expr::Array) -> Result<Self, Self::Error> {
//         let mut values = Vec::with_capacity(array.0.len());
//         for item in array.0 {
//             values.push(ValueProto::try_from(item)?);
//         }
//         Ok(ArrayProto { values })
//     }
// }

// impl TryFrom<ObjectProto> for crate::expr::Object {
//     type Error = anyhow::Error;

//     fn try_from(proto: ObjectProto) -> Result<Self, Self::Error> {
//         let mut object = BTreeMap::new();
//         for (key, value) in proto.values {
//             object.insert(key, crate::expr::Value::try_from(value)?);
//         }
//         Ok(crate::expr::Object(object))
//     }
// }

// impl TryFrom<crate::expr::Object> for ObjectProto {
//     type Error = anyhow::Error;

//     fn try_from(object: crate::expr::Object) -> Result<Self, Self::Error> {
//         let mut values = BTreeMap::new();
//         for (key, value) in object.0 {
//             values.insert(key, ValueProto::try_from(value)?);
//         }
//         Ok(ObjectProto { values })
//     }
// }

// impl TryFrom<GeometryProto> for crate::expr::Geometry {
//     type Error = anyhow::Error;

//     fn try_from(proto: GeometryProto) -> Result<Self, Self::Error> {
//         use crate::protocol::surrealdb::value::geometry;

//         let Some(inner) = proto.inner else {
//             return Err(anyhow::anyhow!("Invalid Geometry: missing value"));
//         };

//         let geometry = match inner {
//             geometry::Inner::Point(v) => crate::expr::Geometry::Point(v.into()),
//             geometry::Inner::Line(v) => crate::expr::Geometry::Line(v.into()),
//             geometry::Inner::Polygon(v) => crate::expr::Geometry::Polygon(v.try_into()?),
//             geometry::Inner::MultiPoint(v) => crate::expr::Geometry::MultiPoint(v.into()),
//             geometry::Inner::MultiLine(v) => crate::expr::Geometry::MultiLine(v.into()),
//             geometry::Inner::MultiPolygon(v) => crate::expr::Geometry::MultiPolygon(v.try_into()?),
//             geometry::Inner::Collection(v) => crate::expr::Geometry::Collection(v.try_into()?),
//         };

//         Ok(geometry)
//     }
// }

// impl TryFrom<crate::expr::Geometry> for GeometryProto {
//     type Error = anyhow::Error;

//     fn try_from(geometry: crate::expr::Geometry) -> Result<Self, Self::Error> {
//         use crate::protocol::surrealdb::value::geometry;

//         let inner = match geometry {
//             crate::expr::Geometry::Point(v) => geometry::Inner::Point(v.into()),
//             crate::expr::Geometry::Line(v) => geometry::Inner::Line(v.into()),
//             crate::expr::Geometry::Polygon(v) => geometry::Inner::Polygon(v.into()),
//             crate::expr::Geometry::MultiPoint(v) => geometry::Inner::MultiPoint(v.into()),
//             crate::expr::Geometry::MultiLine(v) => geometry::Inner::MultiLine(v.into()),
//             crate::expr::Geometry::MultiPolygon(v) => geometry::Inner::MultiPolygon(v.into()),
//             crate::expr::Geometry::Collection(v) => geometry::Inner::Collection(v.try_into()?),
//         };

//         Ok(Self {
//             inner: Some(inner),
//         })
//     }
// }

// impl From<PointProto> for geo::Coord<f64> {
//     fn from(proto: PointProto) -> Self {
//         Self {
//             x: proto.x,
//             y: proto.y,
//         }
//     }
// }

// impl From<geo::Coord<f64>> for PointProto {
//     fn from(coord: geo::Coord<f64>) -> Self {
//         Self {
//             x: coord.x,
//             y: coord.y,
//         }
//     }
// }

// impl From<PointProto> for geo::Point<f64> {
//     fn from(proto: PointProto) -> Self {
//         Self::new(proto.x, proto.y)
//     }
// }

// impl From<geo::Point<f64>> for PointProto {
//     fn from(point: geo::Point<f64>) -> Self {
//         Self {
//             x: point.x(),
//             y: point.y(),
//         }
//     }
// }

// impl From<LineStringProto> for geo::LineString<f64> {
//     fn from(proto: LineStringProto) -> Self {
//         Self(
//             proto.points.into_iter().map(Into::into).collect()
//         )
//     }
// }

// impl From<geo::LineString<f64>> for LineStringProto {
//     fn from(line: geo::LineString<f64>) -> Self {
//         Self {
//             points: line.0.into_iter().map(Into::into).collect(),
//         }
//     }
// }

// impl TryFrom<PolygonProto> for geo::Polygon<f64> {
//     type Error = anyhow::Error;

//     fn try_from(proto: PolygonProto) -> Result<Self, Self::Error> {
//         let Some(exterior) = proto.exterior else {
//             return Err(anyhow::anyhow!("Invalid Polygon: missing exterior"));
//         };
//         let interiors = proto.interiors.into_iter().map(Into::into).collect();
//         Ok(Self::new(exterior.into(), interiors))
//     }
// }

// impl From<geo::Polygon<f64>> for PolygonProto {
//     fn from(polygon: geo::Polygon<f64>) -> Self {
//         Self {
//             exterior: Some(LineStringProto::from(polygon.exterior().clone())),
//             interiors: polygon.interiors().into_iter().map(Clone::clone).map(Into::into).collect(),
//         }
//     }
// }

// impl From<MultiPointProto> for geo::MultiPoint<f64> {
//     fn from(proto: MultiPointProto) -> Self {
//         Self(
//             proto.points.into_iter().map(Into::into).collect()
//         )
//     }
// }

// impl From<geo::MultiPoint<f64>> for MultiPointProto {
//     fn from(multi_point: geo::MultiPoint<f64>) -> Self {
//         Self {
//             points: multi_point.0.into_iter().map(Into::into).collect(),
//         }
//     }
// }

// impl From<MultiLineStringProto> for geo::MultiLineString<f64> {
//     fn from(proto: MultiLineStringProto) -> Self {
//         Self(
//             proto.lines.into_iter().map(Into::into).collect()
//         )
//     }
// }

// impl From<geo::MultiLineString<f64>> for MultiLineStringProto {
//     fn from(multi_line: geo::MultiLineString<f64>) -> Self {
//         Self {
//             lines: multi_line.0.into_iter().map(Into::into).collect(),
//         }
//     }
// }

// impl TryFrom<MultiPolygonProto> for geo::MultiPolygon<f64> {
//     type Error = anyhow::Error;

//     fn try_from(proto: MultiPolygonProto) -> Result<Self, Self::Error> {
//         Ok(Self(
//             proto.polygons.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?
//         ))
//     }
// }

// impl From<geo::MultiPolygon<f64>> for MultiPolygonProto {
//     fn from(multi_polygon: geo::MultiPolygon<f64>) -> Self {
//         Self {
//             polygons: multi_polygon.0.into_iter().map(Into::into).collect(),
//         }
//     }
// }

// impl TryFrom<GeometryCollectionProto> for Vec<crate::expr::Geometry> {
//     type Error = anyhow::Error;

//     fn try_from(proto: GeometryCollectionProto) -> Result<Self, Self::Error> {
//         let mut geometries = Vec::with_capacity(proto.geometries.len());
//         for geometry in proto.geometries {
//             geometries.push(crate::expr::Geometry::try_from(geometry)?);
//         }
//         Ok(geometries)
//     }
// }

// impl TryFrom<Vec<crate::expr::Geometry>> for GeometryCollectionProto {
//     type Error = anyhow::Error;

//     fn try_from(geometries: Vec<crate::expr::Geometry>) -> Result<Self, Self::Error> {
//         let mut proto_geometries = Vec::with_capacity(geometries.len());
//         for geometry in geometries {
//             proto_geometries.push(GeometryProto::try_from(geometry)?);
//         }
//         Ok(GeometryCollectionProto { geometries: proto_geometries })
//     }
// }

// impl TryFrom<RecordIdProto> for crate::expr::Thing {
//     type Error = anyhow::Error;

//     fn try_from(proto: RecordIdProto) -> Result<Self, Self::Error> {
//         let Some(id) = proto.id else {
//             return Err(anyhow::anyhow!("Invalid RecordId: missing id"));
//         };
//         Ok(Self {
//             tb: proto.table,
//             id: id.try_into()?,
//         })
//     }
// }

// impl TryFrom<crate::expr::Thing> for RecordIdProto {
//     type Error = anyhow::Error;

//     fn try_from(thing: crate::expr::Thing) -> Result<Self, Self::Error> {
//         Ok(Self {
//             table: thing.tb,
//             id: Some(thing.id.try_into()?),
//         })
//     }
// }

// impl From<FileProto> for crate::expr::File {
//     fn from(proto: FileProto) -> Self {
//         Self {
//             bucket: proto.bucket,
//             key: proto.key,
//         }
//     }
// }

// impl From<crate::expr::File> for FileProto {
//     fn from(file: crate::expr::File) -> Self {
//         Self {
//             bucket: file.bucket,
//             key: file.key,
//         }
//     }
// }

// impl TryFrom<IdProto> for crate::expr::Id {
//     type Error = anyhow::Error;

//     fn try_from(proto: IdProto) -> Result<Self, Self::Error> {
//         use crate::protocol::surrealdb::value::id;
//         let Some(inner) = proto.inner else {
//             return Err(anyhow::anyhow!("Invalid Id: missing value"));
//         };

//         Ok(match inner {
//             id::Inner::Number(v) => crate::expr::Id::Number(v),
//             id::Inner::String(v) => crate::expr::Id::String(v),
//             id::Inner::Uuid(v) => crate::expr::Id::Uuid(v.try_into()?),
//             id::Inner::Array(v) => crate::expr::Id::Array(v.try_into()?),
//         })
//     }
// }

// impl TryFrom<crate::expr::Id> for IdProto {
//     type Error = anyhow::Error;

//     fn try_from(id: crate::expr::Id) -> Result<Self, Self::Error> {
//         use crate::protocol::surrealdb::value::id;

//         let inner = match id {
//             crate::expr::Id::Number(v) => id::Inner::Number(v),
//             crate::expr::Id::String(v) => id::Inner::String(v),
//             crate::expr::Id::Uuid(v) => id::Inner::Uuid(v.to_string()),
//             crate::expr::Id::Array(v) => id::Inner::Array(v.try_into()?),
//             crate::expr::Id::Generate(v) => {
//                 return Err(anyhow::anyhow!("Id::Generate is not supported in proto conversion: {v:?}"));
//             }
//             crate::expr::Id::Object(v) => {
//                 return Err(anyhow::anyhow!("Id::Object is not supported in proto conversion: {v:?}"));
//             }
//             crate::expr::Id::Range(v) => {
//                 return Err(anyhow::anyhow!("Id::Range is not supported in proto conversion: {v:?}"));
//             }
//         };

//         Ok(Self {
//             inner: Some(inner),
//         })
//     }
// }
