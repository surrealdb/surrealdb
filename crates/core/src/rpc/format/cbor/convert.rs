use ciborium::Value as Data;
use geo::{LineString, Point, Polygon};
use geo_types::{MultiLineString, MultiPoint, MultiPolygon};
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::iter::once;
use std::ops::Bound;
use std::ops::Deref;

use crate::sql::id::range::IdRange;
use crate::sql::Array;
use crate::sql::Datetime;
use crate::sql::Duration;
use crate::sql::Future;
use crate::sql::Geometry;
use crate::sql::Id;
use crate::sql::Number;
use crate::sql::Object;
use crate::sql::Range;
use crate::sql::Thing;
use crate::sql::Uuid;
use crate::sql::Value;
use std::str::FromStr;

// Tags from the spec - https://www.iana.org/assignments/cbor-tags/cbor-tags.xhtml
const TAG_SPEC_DATETIME: u64 = 0;
const TAG_SPEC_UUID: u64 = 37;

// Custom tags
const TAG_NONE: u64 = 6;
const TAG_TABLE: u64 = 7;
const TAG_RECORDID: u64 = 8;
const TAG_STRING_UUID: u64 = 9;
const TAG_STRING_DECIMAL: u64 = 10;
// const TAG_BINARY_DECIMAL: u64 = 11;
const TAG_CUSTOM_DATETIME: u64 = 12;
const TAG_STRING_DURATION: u64 = 13;
const TAG_CUSTOM_DURATION: u64 = 14;
const TAG_FUTURE: u64 = 15;

// Ranges
const TAG_RANGE: u64 = 49;
const TAG_BOUND_INCLUDED: u64 = 50;
const TAG_BOUND_EXCLUDED: u64 = 51;

// Custom Geometries
const TAG_GEOMETRY_POINT: u64 = 88;
const TAG_GEOMETRY_LINE: u64 = 89;
const TAG_GEOMETRY_POLYGON: u64 = 90;
const TAG_GEOMETRY_MULTIPOINT: u64 = 91;
const TAG_GEOMETRY_MULTILINE: u64 = 92;
const TAG_GEOMETRY_MULTIPOLYGON: u64 = 93;
const TAG_GEOMETRY_COLLECTION: u64 = 94;

#[derive(Debug)]
pub struct Cbor(pub Data);

impl TryFrom<Cbor> for Value {
	type Error = &'static str;
	fn try_from(val: Cbor) -> Result<Self, &'static str> {
		match val.0 {
			Data::Null => Ok(Value::Null),
			Data::Bool(v) => Ok(Value::from(v)),
			Data::Integer(v) => Ok(Value::from(i128::from(v))),
			Data::Float(v) => Ok(Value::from(v)),
			Data::Bytes(v) => Ok(Value::Bytes(v.into())),
			Data::Text(v) => Ok(Value::from(v)),
			Data::Array(v) => Ok(Value::Array(Array::try_from(v)?)),
			Data::Map(v) => Ok(Value::Object(Object::try_from(v)?)),
			Data::Tag(t, v) => {
				match t {
					// A literal datetime
					TAG_SPEC_DATETIME => match *v {
						Data::Text(v) => match Datetime::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid Datetime value"),
						},
						_ => Err("Expected a CBOR text data type"),
					},
					// A custom [seconds: i64, nanos: u32] datetime
					TAG_CUSTOM_DATETIME => match *v {
						Data::Array(v) if v.len() == 2 => {
							let mut iter = v.into_iter();

							let seconds = match iter.next() {
								Some(Data::Integer(v)) => match i64::try_from(v) {
									Ok(v) => v,
									_ => return Err("Expected a CBOR integer data type"),
								},
								_ => return Err("Expected a CBOR integer data type"),
							};

							let nanos = match iter.next() {
								Some(Data::Integer(v)) => match u32::try_from(v) {
									Ok(v) => v,
									_ => return Err("Expected a CBOR integer data type"),
								},
								_ => return Err("Expected a CBOR integer data type"),
							};

							match Datetime::try_from((seconds, nanos)) {
								Ok(v) => Ok(v.into()),
								_ => Err("Expected a valid Datetime value"),
							}
						}
						_ => Err("Expected a CBOR array with 2 elements"),
					},
					// A literal NONE
					TAG_NONE => Ok(Value::None),
					// A literal uuid
					TAG_STRING_UUID => match *v {
						Data::Text(v) => match Uuid::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid UUID value"),
						},
						_ => Err("Expected a CBOR text data type"),
					},
					// A byte string uuid
					TAG_SPEC_UUID => v.deref().to_owned().try_into().map(Value::Uuid),
					// A literal decimal
					TAG_STRING_DECIMAL => match *v {
						Data::Text(v) => match Decimal::from_str(v.as_str()) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid Decimal value"),
						},
						_ => Err("Expected a CBOR text data type"),
					},
					// A literal duration
					TAG_STRING_DURATION => match *v {
						Data::Text(v) => match Duration::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid Duration value"),
						},
						_ => Err("Expected a CBOR text data type"),
					},
					// A custom [seconds: Option<i64>, nanos: Option<u32>] duration
					TAG_CUSTOM_DURATION => match *v {
						Data::Array(v) if v.len() <= 2 => {
							let mut iter = v.into_iter();

							let seconds = match iter.next() {
								Some(Data::Integer(v)) => match i64::try_from(v) {
									Ok(v) => v,
									_ => return Err("Expected a CBOR integer data type"),
								},
								_ => 0,
							};

							let nanos = match iter.next() {
								Some(Data::Integer(v)) => match u32::try_from(v) {
									Ok(v) => v,
									_ => return Err("Expected a CBOR integer data type"),
								},
								_ => 0,
							};

							match Duration::new(seconds, nanos) {
								Ok(v) => Ok(v.into()),
								_ => Err("Expected a valid Duration value"),
							}
						}
						_ => Err("Expected a CBOR array with at most 2 elements"),
					},
					// A literal recordid
					TAG_RECORDID => match *v {
						Data::Text(v) => match Thing::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid RecordID value"),
						},
						Data::Array(mut v) if v.len() == 2 => {
							let tb = match Value::try_from(Cbor(v.remove(0))) {
								Ok(Value::Strand(tb)) => tb.0,
								Ok(Value::Table(tb)) => tb.0,
								_ => return Err(
									"Expected the tb of a Record Id to be a String or Table value",
								),
							};

							let id = Id::try_from(v.remove(0))?;

							Ok(Value::Thing(Thing {
								tb,
								id,
							}))
						}
						_ => Err("Expected a CBOR text data type, or a CBOR array with 2 elements"),
					},
					// A literal table
					TAG_TABLE => match *v {
						Data::Text(v) => Ok(Value::Table(v.into())),
						_ => Err("Expected a CBOR text data type"),
					},
					// A range
					TAG_RANGE => Ok(Value::Range(Box::new(Range::try_from(*v)?))),
					TAG_FUTURE => match *v {
						Data::Text(v) => {
							let block = crate::syn::block(v.as_str())
								.map_err(|_| "Failed to parse block")?;
							Ok(Value::Future(Box::new(Future(block))))
						}
						_ => Err("Expected a CBOR text data type"),
					},
					TAG_GEOMETRY_POINT => match *v {
						Data::Array(mut v) if v.len() == 2 => {
							let x = Value::try_from(Cbor(v.remove(0)))?;
							let y = Value::try_from(Cbor(v.remove(0)))?;

							match (x, y) {
								(Value::Number(x), Value::Number(y)) => Ok(Value::Geometry(
									Geometry::Point((x.as_float(), y.as_float()).into()),
								)),
								_ => Err("Expected a CBOR array with 2 decimal values"),
							}
						}
						_ => Err("Expected a CBOR array with 2 decimal values"),
					},
					TAG_GEOMETRY_LINE => match v.deref() {
						Data::Array(v) => {
							let points = v
								.iter()
								.map(|v| match Value::try_from(Cbor(v.clone()))? {
									Value::Geometry(Geometry::Point(v)) => Ok(v),
									_ => Err("Expected a CBOR array with Geometry Point values"),
								})
								.collect::<Result<Vec<Point>, &str>>()?;

							Ok(Value::Geometry(Geometry::Line(LineString::from(points))))
						}
						_ => Err("Expected a CBOR array with Geometry Point values"),
					},
					TAG_GEOMETRY_POLYGON => match v.deref() {
						Data::Array(v) if !v.is_empty() => {
							let lines = v
								.iter()
								.map(|v| match Value::try_from(Cbor(v.clone()))? {
									Value::Geometry(Geometry::Line(v)) => Ok(v),
									_ => Err("Expected a CBOR array with Geometry Line values"),
								})
								.collect::<Result<Vec<LineString>, &str>>()?;

							let exterior = match lines.first() {
								Some(v) => v,
								_ => return Err(
									"Expected a CBOR array with at least one Geometry Line values",
								),
							};
							let interiors = Vec::from(&lines[1..]);

							Ok(Value::Geometry(Geometry::Polygon(Polygon::new(
								exterior.clone(),
								interiors,
							))))
						}
						_ => Err("Expected a CBOR array with at least one Geometry Line values"),
					},
					TAG_GEOMETRY_MULTIPOINT => match v.deref() {
						Data::Array(v) => {
							let points = v
								.iter()
								.map(|v| match Value::try_from(Cbor(v.clone()))? {
									Value::Geometry(Geometry::Point(v)) => Ok(v),
									_ => Err("Expected a CBOR array with Geometry Point values"),
								})
								.collect::<Result<Vec<Point>, &str>>()?;

							Ok(Value::Geometry(Geometry::MultiPoint(MultiPoint::from(points))))
						}
						_ => Err("Expected a CBOR array with Geometry Point values"),
					},
					TAG_GEOMETRY_MULTILINE => match v.deref() {
						Data::Array(v) => {
							let lines = v
								.iter()
								.map(|v| match Value::try_from(Cbor(v.clone()))? {
									Value::Geometry(Geometry::Line(v)) => Ok(v),
									_ => Err("Expected a CBOR array with Geometry Line values"),
								})
								.collect::<Result<Vec<LineString>, &str>>()?;

							Ok(Value::Geometry(Geometry::MultiLine(MultiLineString::new(lines))))
						}
						_ => Err("Expected a CBOR array with Geometry Line values"),
					},
					TAG_GEOMETRY_MULTIPOLYGON => match v.deref() {
						Data::Array(v) => {
							let polygons = v
								.iter()
								.map(|v| match Value::try_from(Cbor(v.clone()))? {
									Value::Geometry(Geometry::Polygon(v)) => Ok(v),
									_ => Err("Expected a CBOR array with Geometry Polygon values"),
								})
								.collect::<Result<Vec<Polygon>, &str>>()?;

							Ok(Value::Geometry(Geometry::MultiPolygon(MultiPolygon::from(
								polygons,
							))))
						}
						_ => Err("Expected a CBOR array with Geometry Polygon values"),
					},
					TAG_GEOMETRY_COLLECTION => match v.deref() {
						Data::Array(v) => {
							let geometries = v
								.iter()
								.map(|v| match Value::try_from(Cbor(v.clone()))? {
									Value::Geometry(v) => Ok(v),
									_ => Err("Expected a CBOR array with Geometry values"),
								})
								.collect::<Result<Vec<Geometry>, &str>>()?;

							Ok(Value::Geometry(Geometry::Collection(geometries)))
						}
						_ => Err("Expected a CBOR array with Geometry values"),
					},
					// An unknown tag
					_ => Err("Encountered an unknown CBOR tag"),
				}
			}
			_ => Err("Encountered an unknown CBOR data type"),
		}
	}
}

impl TryFrom<Value> for Cbor {
	type Error = &'static str;
	fn try_from(val: Value) -> Result<Self, &'static str> {
		match val {
			Value::None => Ok(Cbor(Data::Tag(TAG_NONE, Box::new(Data::Null)))),
			Value::Null => Ok(Cbor(Data::Null)),
			Value::Bool(v) => Ok(Cbor(Data::Bool(v))),
			Value::Number(v) => match v {
				Number::Int(v) => Ok(Cbor(Data::Integer(v.into()))),
				Number::Float(v) => Ok(Cbor(Data::Float(v))),
				Number::Decimal(v) => {
					Ok(Cbor(Data::Tag(TAG_STRING_DECIMAL, Box::new(Data::Text(v.to_string())))))
				}
			},
			Value::Strand(v) => Ok(Cbor(Data::Text(v.0))),
			Value::Duration(v) => {
				let seconds = v.secs();
				let nanos = v.subsec_nanos();

				let tag_value = match (seconds, nanos) {
					(0, 0) => Box::new(Data::Array(vec![])),
					(_, 0) => Box::new(Data::Array(vec![Data::Integer(seconds.into())])),
					_ => Box::new(Data::Array(vec![
						Data::Integer(seconds.into()),
						Data::Integer(nanos.into()),
					])),
				};

				Ok(Cbor(Data::Tag(TAG_CUSTOM_DURATION, tag_value)))
			}
			Value::Datetime(v) => {
				let seconds = v.timestamp();
				let nanos = v.timestamp_subsec_nanos();

				Ok(Cbor(Data::Tag(
					TAG_CUSTOM_DATETIME,
					Box::new(Data::Array(vec![
						Data::Integer(seconds.into()),
						Data::Integer(nanos.into()),
					])),
				)))
			}
			Value::Uuid(v) => {
				Ok(Cbor(Data::Tag(TAG_SPEC_UUID, Box::new(Data::Bytes(v.into_bytes().into())))))
			}
			Value::Array(v) => Ok(Cbor(Data::Array(
				v.into_iter()
					.map(|v| {
						let v = Cbor::try_from(v)?.0;
						Ok(v)
					})
					.collect::<Result<Vec<Data>, &str>>()?,
			))),
			Value::Object(v) => Ok(Cbor(Data::Map(
				v.into_iter()
					.map(|(k, v)| {
						let k = Data::Text(k);
						let v = Cbor::try_from(v)?.0;
						Ok((k, v))
					})
					.collect::<Result<Vec<(Data, Data)>, &str>>()?,
			))),
			Value::Bytes(v) => Ok(Cbor(Data::Bytes(v.into_inner()))),
			Value::Thing(v) => Ok(Cbor(Data::Tag(
				TAG_RECORDID,
				Box::new(Data::Array(vec![
					Data::Text(v.tb),
					match v.id {
						Id::Number(v) => Data::Integer(v.into()),
						Id::String(v) => Data::Text(v),
						Id::Uuid(v) => Cbor::try_from(Value::from(v))?.0,
						Id::Array(v) => Cbor::try_from(Value::from(v))?.0,
						Id::Object(v) => Cbor::try_from(Value::from(v))?.0,
						Id::Generate(_) => {
							return Err("Cannot encode an ungenerated Record ID into CBOR")
						}
						Id::Range(v) => Data::Tag(TAG_RANGE, Box::new(Data::try_from(*v)?)),
					},
				])),
			))),
			Value::Table(v) => Ok(Cbor(Data::Tag(TAG_TABLE, Box::new(Data::Text(v.0))))),
			Value::Geometry(v) => Ok(Cbor(encode_geometry(v)?)),
			Value::Range(v) => Ok(Cbor(Data::Tag(TAG_RANGE, Box::new(Data::try_from(*v)?)))),
			Value::Future(v) => {
				let bin = Data::Text(format!("{}", (*v).0));
				Ok(Cbor(Data::Tag(TAG_FUTURE, Box::new(bin))))
			}
			// We shouldn't reach here
			_ => Err("Found unsupported SurrealQL value being encoded into a CBOR value"),
		}
	}
}

fn encode_geometry(v: Geometry) -> Result<Data, &'static str> {
	match v {
		Geometry::Point(v) => Ok(Data::Tag(
			TAG_GEOMETRY_POINT,
			Box::new(Data::Array(vec![Data::Float(v.x()), Data::Float(v.y())])),
		)),
		Geometry::Line(v) => {
			let data = v
				.points()
				.map(|v| encode_geometry(v.into()))
				.collect::<Result<Vec<Data>, &'static str>>()?;

			Ok(Data::Tag(TAG_GEOMETRY_LINE, Box::new(Data::Array(data))))
		}
		Geometry::Polygon(v) => {
			let data = once(v.exterior())
				.chain(v.interiors())
				.map(|v| encode_geometry(v.clone().into()))
				.collect::<Result<Vec<Data>, &'static str>>()?;

			Ok(Data::Tag(TAG_GEOMETRY_POLYGON, Box::new(Data::Array(data))))
		}
		Geometry::MultiPoint(v) => {
			let data = v
				.iter()
				.map(|v| encode_geometry((*v).into()))
				.collect::<Result<Vec<Data>, &'static str>>()?;

			Ok(Data::Tag(TAG_GEOMETRY_MULTIPOINT, Box::new(Data::Array(data))))
		}
		Geometry::MultiLine(v) => {
			let data = v
				.iter()
				.map(|v| encode_geometry(v.clone().into()))
				.collect::<Result<Vec<Data>, &'static str>>()?;

			Ok(Data::Tag(TAG_GEOMETRY_MULTILINE, Box::new(Data::Array(data))))
		}
		Geometry::MultiPolygon(v) => {
			let data = v
				.iter()
				.map(|v| encode_geometry(v.clone().into()))
				.collect::<Result<Vec<Data>, &'static str>>()?;

			Ok(Data::Tag(TAG_GEOMETRY_MULTIPOLYGON, Box::new(Data::Array(data))))
		}
		Geometry::Collection(v) => {
			let data = v
				.iter()
				.map(|v| encode_geometry(v.clone()))
				.collect::<Result<Vec<Data>, &'static str>>()?;

			Ok(Data::Tag(TAG_GEOMETRY_COLLECTION, Box::new(Data::Array(data))))
		}
	}
}

impl TryFrom<Data> for Range {
	type Error = &'static str;
	fn try_from(val: Data) -> Result<Self, &'static str> {
		fn decode_bound(v: Data) -> Result<Bound<Value>, &'static str> {
			match v {
				Data::Tag(TAG_BOUND_INCLUDED, v) => Ok(Bound::Included(Value::try_from(Cbor(*v))?)),
				Data::Tag(TAG_BOUND_EXCLUDED, v) => Ok(Bound::Excluded(Value::try_from(Cbor(*v))?)),
				Data::Null => Ok(Bound::Unbounded),
				_ => Err("Expected a bound tag"),
			}
		}

		match val {
			Data::Array(v) if v.len() == 2 => {
				let mut v = v;
				let beg = decode_bound(v.remove(0).to_owned())?;
				let end = decode_bound(v.remove(0).to_owned())?;
				Ok(Range::new(beg, end))
			}
			_ => Err("Expected a CBOR array with 2 bounds"),
		}
	}
}

impl TryFrom<Range> for Data {
	type Error = &'static str;
	fn try_from(r: Range) -> Result<Data, &'static str> {
		fn encode(b: Bound<Value>) -> Result<Data, &'static str> {
			match b {
				Bound::Included(v) => {
					Ok(Data::Tag(TAG_BOUND_INCLUDED, Box::new(Cbor::try_from(v)?.0)))
				}
				Bound::Excluded(v) => {
					Ok(Data::Tag(TAG_BOUND_EXCLUDED, Box::new(Cbor::try_from(v)?.0)))
				}
				Bound::Unbounded => Ok(Data::Null),
			}
		}

		Ok(Data::Array(vec![encode(r.beg)?, encode(r.end)?]))
	}
}

impl TryFrom<Data> for IdRange {
	type Error = &'static str;
	fn try_from(val: Data) -> Result<Self, &'static str> {
		fn decode_bound(v: Data) -> Result<Bound<Id>, &'static str> {
			match v {
				Data::Tag(TAG_BOUND_INCLUDED, v) => Ok(Bound::Included(Id::try_from(*v)?)),
				Data::Tag(TAG_BOUND_EXCLUDED, v) => Ok(Bound::Excluded(Id::try_from(*v)?)),
				Data::Null => Ok(Bound::Unbounded),
				_ => Err("Expected a bound tag"),
			}
		}

		match val {
			Data::Array(v) if v.len() == 2 => {
				let mut v = v;
				let beg = decode_bound(v.remove(0).to_owned())?;
				let end = decode_bound(v.remove(0).to_owned())?;
				Ok(IdRange::try_from((beg, end))
					.map_err(|_| "Found an invalid range with ranges as bounds")?)
			}
			_ => Err("Expected a CBOR array with 2 bounds"),
		}
	}
}

impl TryFrom<IdRange> for Data {
	type Error = &'static str;
	fn try_from(r: IdRange) -> Result<Data, &'static str> {
		fn encode(b: Bound<Id>) -> Result<Data, &'static str> {
			match b {
				Bound::Included(v) => Ok(Data::Tag(TAG_BOUND_INCLUDED, Box::new(v.try_into()?))),
				Bound::Excluded(v) => Ok(Data::Tag(TAG_BOUND_EXCLUDED, Box::new(v.try_into()?))),
				Bound::Unbounded => Ok(Data::Null),
			}
		}

		Ok(Data::Array(vec![encode(r.beg)?, encode(r.end)?]))
	}
}

impl TryFrom<Data> for Id {
	type Error = &'static str;
	fn try_from(val: Data) -> Result<Self, &'static str> {
		match val {
			Data::Integer(v) => Ok(Id::Number(i128::from(v) as i64)),
			Data::Text(v) => Ok(Id::String(v)),
			Data::Array(v) => Ok(Id::Array(v.try_into()?)),
			Data::Map(v) => Ok(Id::Object(v.try_into()?)),
			Data::Tag(TAG_RANGE, v) => Ok(Id::Range(Box::new(IdRange::try_from(*v)?))),
			Data::Tag(TAG_STRING_UUID, v) => v.deref().to_owned().try_into().map(Id::Uuid),
			Data::Tag(TAG_SPEC_UUID, v) => v.deref().to_owned().try_into().map(Id::Uuid),
			_ => Err("Expected a CBOR integer, text, array or map"),
		}
	}
}

impl TryFrom<Id> for Data {
	type Error = &'static str;
	fn try_from(v: Id) -> Result<Data, &'static str> {
		match v {
			Id::Number(v) => Ok(Data::Integer(v.into())),
			Id::String(v) => Ok(Data::Text(v)),
			Id::Array(v) => Ok(Cbor::try_from(Value::from(v))?.0),
			Id::Object(v) => Ok(Cbor::try_from(Value::from(v))?.0),
			Id::Range(v) => Ok(Data::Tag(TAG_RANGE, Box::new(v.deref().to_owned().try_into()?))),
			Id::Uuid(v) => {
				Ok(Data::Tag(TAG_SPEC_UUID, Box::new(Data::Bytes(v.into_bytes().into()))))
			}
			Id::Generate(_) => Err("Cannot encode an ungenerated Record ID into CBOR"),
		}
	}
}

impl TryFrom<Vec<Data>> for Array {
	type Error = &'static str;
	fn try_from(val: Vec<Data>) -> Result<Self, &'static str> {
		val.into_iter().map(|v| Value::try_from(Cbor(v))).collect::<Result<Array, &str>>()
	}
}

impl TryFrom<Vec<(Data, Data)>> for Object {
	type Error = &'static str;
	fn try_from(val: Vec<(Data, Data)>) -> Result<Self, &'static str> {
		Ok(Object(
			val.into_iter()
				.map(|(k, v)| {
					let k = Value::try_from(Cbor(k)).map(|k| k.as_raw_string());
					let v = Value::try_from(Cbor(v));
					Ok((k?, v?))
				})
				.collect::<Result<BTreeMap<String, Value>, &str>>()?,
		))
	}
}

impl TryFrom<Data> for Uuid {
	type Error = &'static str;
	fn try_from(val: Data) -> Result<Self, &'static str> {
		match val {
			Data::Bytes(v) if v.len() == 16 => match v.as_slice().try_into() {
				Ok(v) => Ok(Uuid::from(uuid::Uuid::from_bytes(v))),
				Err(_) => Err("Expected a CBOR byte array with 16 elements"),
			},
			_ => Err("Expected a CBOR byte array with 16 elements"),
		}
	}
}
