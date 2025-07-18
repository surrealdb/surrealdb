use ciborium::Value as CborData;
use geo::{LineString, Point, Polygon};
use geo_types::{MultiLineString, MultiPoint, MultiPolygon};
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::iter::once;
use std::ops::{Bound, Deref};

use crate::val::{self, Value};

// Tags from the spec - https://www.iana.org/assignments/cbor-tags/cbor-tags.xhtml
const TAG_SPEC_DATETIME: u64 = 0;
const TAG_SPEC_UUID: u64 = 37;

// Custom tags (6->15 is unassigned)
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

// Ranges (49->51 is unassigned)
const TAG_RANGE: u64 = 49;
const TAG_BOUND_INCLUDED: u64 = 50;
const TAG_BOUND_EXCLUDED: u64 = 51;

// Custom tags (55->60 is unassigned)
const TAG_FILE: u64 = 55;

// Custom Geometries (88->95 is unassigned)
const TAG_GEOMETRY_POINT: u64 = 88;
const TAG_GEOMETRY_LINE: u64 = 89;
const TAG_GEOMETRY_POLYGON: u64 = 90;
const TAG_GEOMETRY_MULTIPOINT: u64 = 91;
const TAG_GEOMETRY_MULTILINE: u64 = 92;
const TAG_GEOMETRY_MULTIPOLYGON: u64 = 93;
const TAG_GEOMETRY_COLLECTION: u64 = 94;

#[derive(Debug)]
pub struct Cbor(pub CborData);

pub fn value_to_cbor(value: Value) -> Result<CborData, &'static str> {
	todo!()
}

pub fn cbor_to_value(value: CborData) -> Result<val::Value, &'static str> {
	todo!()
}

impl TryFrom<Cbor> for val::Value {
	type Error = &'static str;
	fn try_from(val: Cbor) -> Result<Self, &'static str> {
		match val.0 {
			CborData::Null => Ok(val::Value::Null),
			CborData::Bool(v) => Ok(val::Value::from(v)),
			CborData::Integer(v) => Ok(val::Value::from(i128::from(v))),
			CborData::Float(v) => Ok(val::Value::from(v)),
			CborData::Bytes(v) => Ok(val::Value::Bytes(v.into())),
			CborData::Text(v) => Ok(val::Value::from(v)),
			CborData::Array(v) => Ok(val::Value::Array(val::Array::try_from(v)?)),
			CborData::Map(v) => Ok(val::Value::Object(val::Object::try_from(v)?)),
			CborData::Tag(t, v) => {
				match t {
					// A literal datetime
					TAG_SPEC_DATETIME => match *v {
						CborData::Text(v) => match val::Datetime::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid val::Datetime value"),
						},
						_ => Err("Expected a CBOR text data type"),
					},
					// A custom [seconds: i64, nanos: u32] datetime
					TAG_CUSTOM_DATETIME => match *v {
						CborData::Array(v) if v.len() == 2 => {
							let mut iter = v.into_iter();

							let seconds = match iter.next() {
								Some(CborData::Integer(v)) => match i64::try_from(v) {
									Ok(v) => v,
									_ => return Err("Expected a CBOR integer data type"),
								},
								_ => return Err("Expected a CBOR integer data type"),
							};

							let nanos = match iter.next() {
								Some(CborData::Integer(v)) => match u32::try_from(v) {
									Ok(v) => v,
									_ => return Err("Expected a CBOR integer data type"),
								},
								_ => return Err("Expected a CBOR integer data type"),
							};

							match val::Datetime::try_from((seconds, nanos)) {
								Ok(v) => Ok(v.into()),
								_ => Err("Expected a valid val::Datetime value"),
							}
						}
						_ => Err("Expected a CBOR array with 2 elements"),
					},
					// A literal NONE
					TAG_NONE => Ok(val::Value::None),
					// A literal uuid
					TAG_STRING_UUID => match *v {
						CborData::Text(v) => match val::Uuid::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid UUID value"),
						},
						_ => Err("Expected a CBOR text data type"),
					},
					// A byte string uuid
					TAG_SPEC_UUID => v.deref().to_owned().try_into().map(val::Value::Uuid),
					// A literal decimal
					TAG_STRING_DECIMAL => match *v {
						CborData::Text(v) => match Decimal::from_str_normalized(v.as_str()) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid Decimal value"),
						},
						_ => Err("Expected a CBOR text data type"),
					},
					// A literal duration
					TAG_STRING_DURATION => match *v {
						CborData::Text(v) => match val::Duration::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid val::Duration value"),
						},
						_ => Err("Expected a CBOR text data type"),
					},
					// A custom [seconds: Option<u64>, nanos: Option<u32>] duration
					TAG_CUSTOM_DURATION => match *v {
						CborData::Array(v) if v.len() <= 2 => {
							let mut iter = v.into_iter();

							let seconds = match iter.next() {
								Some(CborData::Integer(v)) => match u64::try_from(v) {
									Ok(v) => v,
									_ => return Err("Expected a CBOR integer data type"),
								},
								_ => 0,
							};

							let nanos = match iter.next() {
								Some(CborData::Integer(v)) => match u32::try_from(v) {
									Ok(v) => v,
									_ => return Err("Expected a CBOR integer data type"),
								},
								_ => 0,
							};

							Ok(val::Duration::new(seconds, nanos).into())
						}
						_ => Err("Expected a CBOR array with at most 2 elements"),
					},
					// A literal recordid
					TAG_RECORDID => match *v {
						CborData::Text(v) => match val::RecordId::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid RecordID value"),
						},
						CborData::Array(mut v) if v.len() == 2 => {
							let tb = match val::Value::try_from(Cbor(v.remove(0))) {
								Ok(val::Value::Strand(tb)) => tb.0,
								Ok(val::Value::Table(tb)) => tb.0,
								_ => {
									return Err(
										"Expected the tb of a Record Id to be a String or Table value",
									);
								}
							};

							let id = val::RecordIdKey::try_from(v.remove(0))?;

							Ok(val::Value::Thing(val::Thing {
								tb,
								id,
							}))
						}
						_ => Err("Expected a CBOR text data type, or a CBOR array with 2 elements"),
					},
					// A literal table
					/*
					TAG_TABLE => match *v {
						CborData::Text(v) => Ok(val::Value::Table(v.into())),
						_ => Err("Expected a CBOR text data type"),
					},
					*/
					// A range
					TAG_RANGE => Ok(val::Value::Range(Box::new(val::Range::try_from(*v)?))),
					/*
					TAG_FUTURE => match *v {
						CborData::Text(v) => {
							let block = crate::syn::block(v.as_str())
								.map_err(|_| "Failed to parse block")?;
							Ok(val::Value::Future(Box::new(val::Future(block))))
						}
						_ => Err("Expected a CBOR text data type"),
					},*/
					TAG_GEOMETRY_POINT => match *v {
						CborData::Array(mut v) if v.len() == 2 => {
							let x = val::Value::try_from(Cbor(v.remove(0)))?;
							let y = val::Value::try_from(Cbor(v.remove(0)))?;

							match (x, y) {
								(val::Value::Number(x), val::Value::Number(y)) => {
									Ok(val::Value::Geometry(val::Geometry::Point(
										(x.as_float(), y.as_float()).into(),
									)))
								}
								_ => Err("Expected a CBOR array with 2 decimal values"),
							}
						}
						_ => Err("Expected a CBOR array with 2 decimal values"),
					},
					TAG_GEOMETRY_LINE => match v.deref() {
						CborData::Array(v) => {
							let points = v
								.iter()
								.map(|v| match val::Value::try_from(Cbor(v.clone()))? {
									val::Value::Geometry(val::Geometry::Point(v)) => Ok(v),
									_ => Err("Expected a CBOR array with Geometry Point values"),
								})
								.collect::<Result<Vec<Point>, &str>>()?;

							Ok(val::Value::Geometry(val::Geometry::Line(LineString::from(points))))
						}
						_ => Err("Expected a CBOR array with Geometry Point values"),
					},
					TAG_GEOMETRY_POLYGON => match v.deref() {
						CborData::Array(v) if !v.is_empty() => {
							let lines = v
								.iter()
								.map(|v| match val::Value::try_from(Cbor(v.clone()))? {
									val::Value::Geometry(val::Geometry::Line(v)) => Ok(v),
									_ => Err("Expected a CBOR array with Geometry Line values"),
								})
								.collect::<Result<Vec<LineString>, &str>>()?;

							let exterior = match lines.first() {
								Some(v) => v,
								_ => {
									return Err(
										"Expected a CBOR array with at least one Geometry Line values",
									);
								}
							};
							let interiors = Vec::from(&lines[1..]);

							Ok(val::Value::Geometry(val::Geometry::Polygon(Polygon::new(
								exterior.clone(),
								interiors,
							))))
						}
						_ => Err("Expected a CBOR array with at least one Geometry Line values"),
					},
					TAG_GEOMETRY_MULTIPOINT => match v.deref() {
						CborData::Array(v) => {
							let points = v
								.iter()
								.map(|v| match val::Value::try_from(Cbor(v.clone()))? {
									val::Value::Geometry(val::Geometry::Point(v)) => Ok(v),
									_ => Err("Expected a CBOR array with Geometry Point values"),
								})
								.collect::<Result<Vec<Point>, &str>>()?;

							Ok(val::Value::Geometry(val::Geometry::MultiPoint(MultiPoint::from(
								points,
							))))
						}
						_ => Err("Expected a CBOR array with Geometry Point values"),
					},
					TAG_GEOMETRY_MULTILINE => match v.deref() {
						CborData::Array(v) => {
							let lines = v
								.iter()
								.map(|v| match val::Value::try_from(Cbor(v.clone()))? {
									val::Value::Geometry(val::Geometry::Line(v)) => Ok(v),
									_ => Err("Expected a CBOR array with Geometry Line values"),
								})
								.collect::<Result<Vec<LineString>, &str>>()?;

							Ok(val::Value::Geometry(val::Geometry::MultiLine(
								MultiLineString::new(lines),
							)))
						}
						_ => Err("Expected a CBOR array with Geometry Line values"),
					},
					TAG_GEOMETRY_MULTIPOLYGON => match v.deref() {
						CborData::Array(v) => {
							let polygons = v
								.iter()
								.map(|v| match val::Value::try_from(Cbor(v.clone()))? {
									val::Value::Geometry(val::Geometry::Polygon(v)) => Ok(v),
									_ => Err("Expected a CBOR array with Geometry Polygon values"),
								})
								.collect::<Result<Vec<Polygon>, &str>>()?;

							Ok(val::Value::Geometry(val::Geometry::MultiPolygon(
								MultiPolygon::from(polygons),
							)))
						}
						_ => Err("Expected a CBOR array with Geometry Polygon values"),
					},
					TAG_GEOMETRY_COLLECTION => match v.deref() {
						CborData::Array(v) => {
							let geometries = v
								.iter()
								.map(|v| match val::Value::try_from(Cbor(v.clone()))? {
									val::Value::Geometry(v) => Ok(v),
									_ => Err("Expected a CBOR array with Geometry values"),
								})
								.collect::<Result<Vec<val::Geometry>, &str>>()?;

							Ok(val::Value::Geometry(val::Geometry::Collection(geometries)))
						}
						_ => Err("Expected a CBOR array with Geometry values"),
					},
					TAG_FILE => match *v {
						CborData::Array(mut v) if v.len() == 2 => {
							let CborData::Text(bucket) = v.remove(0) else {
								return Err("Expected the bucket name to be a string value");
							};

							let CborData::Text(key) = v.remove(0) else {
								return Err("Expected the file key to be a string value");
							};

							Ok(val::Value::File(val::File {
								bucket,
								key,
							}))
						}
						_ => Err("Expected a CBOR array with two String bucket and key values"),
					},
					// An unknown tag
					_ => Err("Encountered an unknown CBOR tag"),
				}
			}
			_ => Err("Encountered an unknown CBOR data type"),
		}
	}
}

impl TryFrom<val::Value> for Cbor {
	type Error = &'static str;
	fn try_from(val: val::Value) -> Result<Self, &'static str> {
		match val {
			val::Value::None => Ok(Cbor(CborData::Tag(TAG_NONE, Box::new(CborData::Null)))),
			val::Value::Null => Ok(Cbor(CborData::Null)),
			val::Value::Bool(v) => Ok(Cbor(CborData::Bool(v))),
			val::Value::Number(v) => match v {
				val::Number::Int(v) => Ok(Cbor(CborData::Integer(v.into()))),
				val::Number::Float(v) => Ok(Cbor(CborData::Float(v))),
				val::Number::Decimal(v) => Ok(Cbor(CborData::Tag(
					TAG_STRING_DECIMAL,
					Box::new(CborData::Text(v.to_string())),
				))),
			},
			val::Value::Strand(v) => Ok(Cbor(CborData::Text(v.0))),
			val::Value::Duration(v) => {
				let seconds = v.secs();
				let nanos = v.subsec_nanos();

				let tag_value = match (seconds, nanos) {
					(0, 0) => Box::new(CborData::Array(vec![])),
					(_, 0) => Box::new(CborData::Array(vec![CborData::Integer(seconds.into())])),
					_ => Box::new(CborData::Array(vec![
						CborData::Integer(seconds.into()),
						CborData::Integer(nanos.into()),
					])),
				};

				Ok(Cbor(CborData::Tag(TAG_CUSTOM_DURATION, tag_value)))
			}
			val::Value::Datetime(v) => {
				let seconds = v.timestamp();
				let nanos = v.timestamp_subsec_nanos();

				Ok(Cbor(CborData::Tag(
					TAG_CUSTOM_DATETIME,
					Box::new(CborData::Array(vec![
						CborData::Integer(seconds.into()),
						CborData::Integer(nanos.into()),
					])),
				)))
			}
			val::Value::Uuid(v) => Ok(Cbor(CborData::Tag(
				TAG_SPEC_UUID,
				Box::new(CborData::Bytes(v.into_bytes().into())),
			))),
			val::Value::Array(v) => Ok(Cbor(CborData::Array(
				v.into_iter()
					.map(|v| {
						let v = Cbor::try_from(v)?.0;
						Ok(v)
					})
					.collect::<Result<Vec<CborData>, &str>>()?,
			))),
			val::Value::Object(v) => Ok(Cbor(CborData::Map(
				v.into_iter()
					.map(|(k, v)| {
						let k = CborData::Text(k);
						let v = Cbor::try_from(v)?.0;
						Ok((k, v))
					})
					.collect::<Result<Vec<(CborData, CborData)>, &str>>()?,
			))),
			val::Value::Bytes(v) => Ok(Cbor(CborData::Bytes(v.into_inner()))),
			val::Value::Thing(v) => Ok(Cbor(CborData::Tag(
				TAG_RECORDID,
				Box::new(CborData::Array(vec![
					CborData::Text(v.tb),
					match v.id {
						val::RecordIdKey::Number(v) => CborData::Integer(v.into()),
						val::RecordIdKey::String(v) => CborData::Text(v),
						val::RecordIdKey::Uuid(v) => Cbor::try_from(val::Value::from(v))?.0,
						val::RecordIdKey::Array(v) => Cbor::try_from(val::Value::from(v))?.0,
						val::RecordIdKey::Object(v) => Cbor::try_from(val::Value::from(v))?.0,
						val::RecordIdKeyt::Range(v) => {
							CborData::Tag(TAG_RANGE, Box::new(CborData::try_from(*v)?))
						}
					},
				])),
			))),
			val::Value::Table(v) => {
				Ok(Cbor(CborData::Tag(TAG_TABLE, Box::new(CborData::Text(v.0)))))
			}
			val::Value::Geometry(v) => Ok(Cbor(encode_geometry(v)?)),
			val::Value::Range(v) => {
				Ok(Cbor(CborData::Tag(TAG_RANGE, Box::new(CborData::try_from(*v)?))))
			}
			val::Value::Future(v) => {
				let bin = CborData::Text(format!("{}", (*v).0));
				Ok(Cbor(CborData::Tag(TAG_FUTURE, Box::new(bin))))
			}
			val::Value::File(val::File {
				bucket,
				key,
			}) => Ok(Cbor(CborData::Tag(
				TAG_FILE,
				Box::new(CborData::Array(vec![CborData::Text(bucket), CborData::Text(key)])),
			))),
			// We shouldn't reach here
			_ => Err("Found unsupported SurrealQL value being encoded into a CBOR value"),
		}
	}
}

fn encode_geometry(v: val::Geometry) -> Result<CborData, &'static str> {
	match v {
		val::Geometry::Point(v) => Ok(CborData::Tag(
			TAG_GEOMETRY_POINT,
			Box::new(CborData::Array(vec![CborData::Float(v.x()), CborData::Float(v.y())])),
		)),
		val::Geometry::Line(v) => {
			let data = v
				.points()
				.map(|v| encode_geometry(v.into()))
				.collect::<Result<Vec<CborData>, &'static str>>()?;

			Ok(CborData::Tag(TAG_GEOMETRY_LINE, Box::new(CborData::Array(data))))
		}
		val::Geometry::Polygon(v) => {
			let data = once(v.exterior())
				.chain(v.interiors())
				.map(|v| encode_geometry(v.clone().into()))
				.collect::<Result<Vec<CborData>, &'static str>>()?;

			Ok(CborData::Tag(TAG_GEOMETRY_POLYGON, Box::new(CborData::Array(data))))
		}
		val::Geometry::MultiPoint(v) => {
			let data = v
				.iter()
				.map(|v| encode_geometry((*v).into()))
				.collect::<Result<Vec<CborData>, &'static str>>()?;

			Ok(CborData::Tag(TAG_GEOMETRY_MULTIPOINT, Box::new(CborData::Array(data))))
		}
		val::Geometry::MultiLine(v) => {
			let data = v
				.iter()
				.map(|v| encode_geometry(v.clone().into()))
				.collect::<Result<Vec<CborData>, &'static str>>()?;

			Ok(CborData::Tag(TAG_GEOMETRY_MULTILINE, Box::new(CborData::Array(data))))
		}
		val::Geometry::MultiPolygon(v) => {
			let data = v
				.iter()
				.map(|v| encode_geometry(v.clone().into()))
				.collect::<Result<Vec<CborData>, &'static str>>()?;

			Ok(CborData::Tag(TAG_GEOMETRY_MULTIPOLYGON, Box::new(CborData::Array(data))))
		}
		val::Geometry::Collection(v) => {
			let data = v
				.iter()
				.map(|v| encode_geometry(v.clone()))
				.collect::<Result<Vec<CborData>, &'static str>>()?;

			Ok(CborData::Tag(TAG_GEOMETRY_COLLECTION, Box::new(CborData::Array(data))))
		}
	}
}

impl TryFrom<CborData> for val::Range {
	type Error = &'static str;
	fn try_from(val: CborData) -> Result<Self, &'static str> {
		fn decode_bound(v: CborData) -> Result<Bound<val::Value>, &'static str> {
			match v {
				CborData::Tag(TAG_BOUND_INCLUDED, v) => {
					Ok(Bound::Included(val::Value::try_from(Cbor(*v))?))
				}
				CborData::Tag(TAG_BOUND_EXCLUDED, v) => {
					Ok(Bound::Excluded(val::Value::try_from(Cbor(*v))?))
				}
				CborData::Null => Ok(Bound::Unbounded),
				_ => Err("Expected a bound tag"),
			}
		}

		match val {
			CborData::Array(v) if v.len() == 2 => {
				let mut v = v;
				let beg = decode_bound(v.remove(0).clone())?;
				let end = decode_bound(v.remove(0).clone())?;
				Ok(val::Range::new(beg, end))
			}
			_ => Err("Expected a CBOR array with 2 bounds"),
		}
	}
}

impl TryFrom<val::Range> for CborData {
	type Error = &'static str;
	fn try_from(r: val::Range) -> Result<CborData, &'static str> {
		fn encode(b: Bound<val::Value>) -> Result<CborData, &'static str> {
			match b {
				Bound::Included(v) => {
					Ok(CborData::Tag(TAG_BOUND_INCLUDED, Box::new(Cbor::try_from(v)?.0)))
				}
				Bound::Excluded(v) => {
					Ok(CborData::Tag(TAG_BOUND_EXCLUDED, Box::new(Cbor::try_from(v)?.0)))
				}
				Bound::Unbounded => Ok(CborData::Null),
			}
		}

		Ok(CborData::Array(vec![encode(r.beg)?, encode(r.end)?]))
	}
}

impl TryFrom<CborData> for val::id::range::RecordIdKeyRangeLit {
	type Error = &'static str;
	fn try_from(val: CborData) -> Result<Self, &'static str> {
		fn decode_bound(v: CborData) -> Result<Bound<val::RecordIdKeyLit>, &'static str> {
			match v {
				CborData::Tag(TAG_BOUND_INCLUDED, v) => {
					Ok(Bound::Included(val::RecordIdKeyLit::try_from(*v)?))
				}
				CborData::Tag(TAG_BOUND_EXCLUDED, v) => {
					Ok(Bound::Excluded(val::RecordIdKeyLit::try_from(*v)?))
				}
				CborData::Null => Ok(Bound::Unbounded),
				_ => Err("Expected a bound tag"),
			}
		}

		match val {
			CborData::Array(v) if v.len() == 2 => {
				let mut v = v;
				let beg = decode_bound(v.remove(0).clone())?;
				let end = decode_bound(v.remove(0).clone())?;
				Ok(val::id::range::RecordIdKeyRangeLit::try_from((beg, end))
					.map_err(|_| "Found an invalid range with ranges as bounds")?)
			}
			_ => Err("Expected a CBOR array with 2 bounds"),
		}
	}
}

impl TryFrom<val::id::range::RecordIdKeyRangeLit> for CborData {
	type Error = &'static str;
	fn try_from(r: val::id::range::RecordIdKeyRangeLit) -> Result<CborData, &'static str> {
		fn encode(b: Bound<val::RecordIdKeyLit>) -> Result<CborData, &'static str> {
			match b {
				Bound::Included(v) => {
					Ok(CborData::Tag(TAG_BOUND_INCLUDED, Box::new(v.try_into()?)))
				}
				Bound::Excluded(v) => {
					Ok(CborData::Tag(TAG_BOUND_EXCLUDED, Box::new(v.try_into()?)))
				}
				Bound::Unbounded => Ok(CborData::Null),
			}
		}

		Ok(CborData::Array(vec![encode(r.start)?, encode(r.end)?]))
	}
}

impl TryFrom<CborData> for val::RecordIdKeyLit {
	type Error = &'static str;
	fn try_from(val: CborData) -> Result<Self, &'static str> {
		match val {
			CborData::Integer(v) => Ok(val::RecordIdKeyLit::Number(i128::from(v) as i64)),
			CborData::Text(v) => Ok(val::RecordIdKeyLit::String(v)),
			CborData::Array(v) => Ok(val::RecordIdKeyLit::Array(v.try_into()?)),
			CborData::Map(v) => Ok(val::RecordIdKeyLit::Object(v.try_into()?)),
			CborData::Tag(TAG_RANGE, v) => Ok(val::RecordIdKeyLit::Range(Box::new(
				val::id::range::RecordIdKeyRangeLit::try_from(*v)?,
			))),
			CborData::Tag(TAG_STRING_UUID, v) => {
				v.deref().to_owned().try_into().map(val::RecordIdKeyLit::Uuid)
			}
			CborData::Tag(TAG_SPEC_UUID, v) => {
				v.deref().to_owned().try_into().map(val::RecordIdKeyLit::Uuid)
			}
			_ => Err("Expected a CBOR integer, text, array or map"),
		}
	}
}

impl TryFrom<val::RecordIdKeyLit> for CborData {
	type Error = &'static str;
	fn try_from(v: val::RecordIdKeyLit) -> Result<CborData, &'static str> {
		match v {
			val::RecordIdKeyLit::Number(v) => Ok(CborData::Integer(v.into())),
			val::RecordIdKeyLit::String(v) => Ok(CborData::Text(v)),
			val::RecordIdKeyLit::Array(v) => Ok(Cbor::try_from(val::Value::from(v))?.0),
			val::RecordIdKeyLit::Object(v) => Ok(Cbor::try_from(val::Value::from(v))?.0),
			val::RecordIdKeyLit::Range(v) => {
				Ok(CborData::Tag(TAG_RANGE, Box::new(v.deref().to_owned().try_into()?)))
			}
			val::RecordIdKeyLit::Uuid(v) => {
				Ok(CborData::Tag(TAG_SPEC_UUID, Box::new(CborData::Bytes(v.into_bytes().into()))))
			}
			val::RecordIdKeyLit::Generate(_) => {
				Err("Cannot encode an ungenerated Record ID into CBOR")
			}
		}
	}
}

impl TryFrom<Vec<CborData>> for val::Array {
	type Error = &'static str;
	fn try_from(val: Vec<CborData>) -> Result<Self, &'static str> {
		val.into_iter().map(|v| val::Value::try_from(Cbor(v))).collect::<Result<val::Array, &str>>()
	}
}

impl TryFrom<Vec<(CborData, CborData)>> for val::Object {
	type Error = &'static str;
	fn try_from(val: Vec<(CborData, CborData)>) -> Result<Self, &'static str> {
		Ok(val::Object(
			val.into_iter()
				.map(|(k, v)| {
					let k = val::Value::try_from(Cbor(k)).map(|k| k.as_raw_string());
					let v = val::Value::try_from(Cbor(v));
					Ok((k?, v?))
				})
				.collect::<Result<BTreeMap<String, val::Value>, &str>>()?,
		))
	}
}

impl TryFrom<CborData> for val::Uuid {
	type Error = &'static str;
	fn try_from(val: CborData) -> Result<Self, &'static str> {
		match val {
			CborData::Bytes(v) if v.len() == 16 => match v.as_slice().try_into() {
				Ok(v) => Ok(val::Uuid::from(uuid::Uuid::from_bytes(v))),
				Err(_) => Err("Expected a CBOR byte array with 16 elements"),
			},
			_ => Err("Expected a CBOR byte array with 16 elements"),
		}
	}
}

pub mod convert_expr {
	use super::*;
	use crate::expr;

	impl TryFrom<Cbor> for val::Value {
		type Error = &'static str;
		fn try_from(val: Cbor) -> Result<Self, &'static str> {
			match val.0 {
				CborData::Null => Ok(expr::Value::Null),
				CborData::Bool(v) => Ok(expr::Value::from(v)),
				CborData::Integer(v) => Ok(expr::Value::from(i128::from(v))),
				CborData::Float(v) => Ok(expr::Value::from(v)),
				CborData::Bytes(v) => Ok(expr::Value::Bytes(v.into())),
				CborData::Text(v) => Ok(expr::Value::from(v)),
				CborData::Array(v) => Ok(expr::Value::Array(expr::Array::try_from(v)?)),
				CborData::Map(v) => Ok(expr::Value::Object(expr::Object::try_from(v)?)),
				CborData::Tag(t, v) => {
					match t {
						// A literal datetime
						TAG_SPEC_DATETIME => match *v {
							CborData::Text(v) => match expr::Datetime::try_from(v) {
								Ok(v) => Ok(v.into()),
								_ => Err("Expected a valid expr::Datetime value"),
							},
							_ => Err("Expected a CBOR text data type"),
						},
						// A custom [seconds: i64, nanos: u32] datetime
						TAG_CUSTOM_DATETIME => match *v {
							CborData::Array(v) if v.len() == 2 => {
								let mut iter = v.into_iter();

								let seconds = match iter.next() {
									Some(CborData::Integer(v)) => match i64::try_from(v) {
										Ok(v) => v,
										_ => return Err("Expected a CBOR integer data type"),
									},
									_ => return Err("Expected a CBOR integer data type"),
								};

								let nanos = match iter.next() {
									Some(CborData::Integer(v)) => match u32::try_from(v) {
										Ok(v) => v,
										_ => return Err("Expected a CBOR integer data type"),
									},
									_ => return Err("Expected a CBOR integer data type"),
								};

								match expr::Datetime::try_from((seconds, nanos)) {
									Ok(v) => Ok(v.into()),
									_ => Err("Expected a valid expr::Datetime value"),
								}
							}
							_ => Err("Expected a CBOR array with 2 elements"),
						},
						// A literal NONE
						TAG_NONE => Ok(expr::Value::None),
						// A literal uuid
						TAG_STRING_UUID => match *v {
							CborData::Text(v) => match expr::Uuid::try_from(v) {
								Ok(v) => Ok(v.into()),
								_ => Err("Expected a valid UUID value"),
							},
							_ => Err("Expected a CBOR text data type"),
						},
						// A byte string uuid
						TAG_SPEC_UUID => v.deref().to_owned().try_into().map(expr::Value::Uuid),
						// A literal decimal
						TAG_STRING_DECIMAL => match *v {
							CborData::Text(v) => match Decimal::from_str_normalized(v.as_str()) {
								Ok(v) => Ok(v.into()),
								_ => Err("Expected a valid Decimal value"),
							},
							_ => Err("Expected a CBOR text data type"),
						},
						// A literal duration
						TAG_STRING_DURATION => match *v {
							CborData::Text(v) => match expr::Duration::try_from(v) {
								Ok(v) => Ok(v.into()),
								_ => Err("Expected a valid expr::Duration value"),
							},
							_ => Err("Expected a CBOR text data type"),
						},
						// A custom [seconds: Option<u64>, nanos: Option<u32>] duration
						TAG_CUSTOM_DURATION => match *v {
							CborData::Array(v) if v.len() <= 2 => {
								let mut iter = v.into_iter();

								let seconds = match iter.next() {
									Some(CborData::Integer(v)) => match u64::try_from(v) {
										Ok(v) => v,
										_ => return Err("Expected a CBOR integer data type"),
									},
									_ => 0,
								};

								let nanos = match iter.next() {
									Some(CborData::Integer(v)) => match u32::try_from(v) {
										Ok(v) => v,
										_ => return Err("Expected a CBOR integer data type"),
									},
									_ => 0,
								};

								Ok(expr::Duration::new(seconds, nanos).into())
							}
							_ => Err("Expected a CBOR array with at most 2 elements"),
						},
						// A literal recordid
						TAG_RECORDID => match *v {
							CborData::Text(v) => match expr::Thing::try_from(v) {
								Ok(v) => Ok(v.into()),
								_ => Err("Expected a valid RecordID value"),
							},
							CborData::Array(mut v) if v.len() == 2 => {
								let tb = match expr::Value::try_from(Cbor(v.remove(0))) {
									Ok(expr::Value::Strand(tb)) => tb.0,
									Ok(expr::Value::Table(tb)) => tb.0,
									_ => {
										return Err(
											"Expected the tb of a Record Id to be a String or Table value",
										);
									}
								};

								let id = expr::RecordIdKeyLit::try_from(v.remove(0))?;

								Ok(expr::Value::Thing(expr::Thing {
									tb,
									id,
								}))
							}
							_ => Err(
								"Expected a CBOR text data type, or a CBOR array with 2 elements",
							),
						},
						// A literal table
						TAG_TABLE => match *v {
							CborData::Text(v) => Ok(expr::Value::Table(v.into())),
							_ => Err("Expected a CBOR text data type"),
						},
						// A range
						TAG_RANGE => Ok(expr::Value::Range(Box::new(expr::Range::try_from(*v)?))),
						TAG_FUTURE => match *v {
							CborData::Text(v) => {
								let block = crate::syn::block(v.as_str())
									.map_err(|_| "Failed to parse block")?;
								Ok(expr::Value::Future(Box::new(expr::Future(block.into()))))
							}
							_ => Err("Expected a CBOR text data type"),
						},
						TAG_GEOMETRY_POINT => match *v {
							CborData::Array(mut v) if v.len() == 2 => {
								let x = expr::Value::try_from(Cbor(v.remove(0)))?;
								let y = expr::Value::try_from(Cbor(v.remove(0)))?;

								match (x, y) {
									(expr::Value::Number(x), expr::Value::Number(y)) => {
										Ok(expr::Value::Geometry(expr::Geometry::Point(
											(x.as_float(), y.as_float()).into(),
										)))
									}
									_ => Err("Expected a CBOR array with 2 decimal values"),
								}
							}
							_ => Err("Expected a CBOR array with 2 decimal values"),
						},
						TAG_GEOMETRY_LINE => match v.deref() {
							CborData::Array(v) => {
								let points = v
									.iter()
									.map(|v| match expr::Value::try_from(Cbor(v.clone()))? {
										expr::Value::Geometry(expr::Geometry::Point(v)) => Ok(v),
										_ => {
											Err("Expected a CBOR array with Geometry Point values")
										}
									})
									.collect::<Result<Vec<Point>, &str>>()?;

								Ok(expr::Value::Geometry(expr::Geometry::Line(LineString::from(
									points,
								))))
							}
							_ => Err("Expected a CBOR array with Geometry Point values"),
						},
						TAG_GEOMETRY_POLYGON => match v.deref() {
							CborData::Array(v) if !v.is_empty() => {
								let lines = v
									.iter()
									.map(|v| match expr::Value::try_from(Cbor(v.clone()))? {
										expr::Value::Geometry(expr::Geometry::Line(v)) => Ok(v),
										_ => Err("Expected a CBOR array with Geometry Line values"),
									})
									.collect::<Result<Vec<LineString>, &str>>()?;

								let exterior = match lines.first() {
									Some(v) => v,
									_ => {
										return Err(
											"Expected a CBOR array with at least one Geometry Line values",
										);
									}
								};
								let interiors = Vec::from(&lines[1..]);

								Ok(expr::Value::Geometry(expr::Geometry::Polygon(Polygon::new(
									exterior.clone(),
									interiors,
								))))
							}
							_ => {
								Err("Expected a CBOR array with at least one Geometry Line values")
							}
						},
						TAG_GEOMETRY_MULTIPOINT => match v.deref() {
							CborData::Array(v) => {
								let points = v
									.iter()
									.map(|v| match expr::Value::try_from(Cbor(v.clone()))? {
										expr::Value::Geometry(expr::Geometry::Point(v)) => Ok(v),
										_ => {
											Err("Expected a CBOR array with Geometry Point values")
										}
									})
									.collect::<Result<Vec<Point>, &str>>()?;

								Ok(expr::Value::Geometry(expr::Geometry::MultiPoint(
									MultiPoint::from(points),
								)))
							}
							_ => Err("Expected a CBOR array with Geometry Point values"),
						},
						TAG_GEOMETRY_MULTILINE => match v.deref() {
							CborData::Array(v) => {
								let lines = v
									.iter()
									.map(|v| match expr::Value::try_from(Cbor(v.clone()))? {
										expr::Value::Geometry(expr::Geometry::Line(v)) => Ok(v),
										_ => Err("Expected a CBOR array with Geometry Line values"),
									})
									.collect::<Result<Vec<LineString>, &str>>()?;

								Ok(expr::Value::Geometry(expr::Geometry::MultiLine(
									MultiLineString::new(lines),
								)))
							}
							_ => Err("Expected a CBOR array with Geometry Line values"),
						},
						TAG_GEOMETRY_MULTIPOLYGON => match v.deref() {
							CborData::Array(v) => {
								let polygons = v
									.iter()
									.map(|v| match expr::Value::try_from(Cbor(v.clone()))? {
										expr::Value::Geometry(expr::Geometry::Polygon(v)) => Ok(v),
										_ => Err(
											"Expected a CBOR array with Geometry Polygon values",
										),
									})
									.collect::<Result<Vec<Polygon>, &str>>()?;

								Ok(expr::Value::Geometry(expr::Geometry::MultiPolygon(
									MultiPolygon::from(polygons),
								)))
							}
							_ => Err("Expected a CBOR array with Geometry Polygon values"),
						},
						TAG_GEOMETRY_COLLECTION => match v.deref() {
							CborData::Array(v) => {
								let geometries = v
									.iter()
									.map(|v| match expr::Value::try_from(Cbor(v.clone()))? {
										expr::Value::Geometry(v) => Ok(v),
										_ => Err("Expected a CBOR array with Geometry values"),
									})
									.collect::<Result<Vec<expr::Geometry>, &str>>()?;

								Ok(expr::Value::Geometry(expr::Geometry::Collection(geometries)))
							}
							_ => Err("Expected a CBOR array with Geometry values"),
						},
						TAG_FILE => match *v {
							CborData::Array(mut v) if v.len() == 2 => {
								let CborData::Text(bucket) = v.remove(0) else {
									return Err("Expected the bucket name to be a string value");
								};

								let CborData::Text(key) = v.remove(0) else {
									return Err("Expected the file key to be a string value");
								};

								Ok(expr::Value::File(expr::File {
									bucket,
									key,
								}))
							}
							_ => Err("Expected a CBOR array with two String bucket and key values"),
						},
						// An unknown tag
						_ => Err("Encountered an unknown CBOR tag"),
					}
				}
				_ => Err("Encountered an unknown CBOR data type"),
			}
		}
	}

	impl TryFrom<expr::Value> for Cbor {
		type Error = &'static str;
		fn try_from(val: expr::Value) -> Result<Self, &'static str> {
			match val {
				expr::Value::None => Ok(Cbor(CborData::Tag(TAG_NONE, Box::new(CborData::Null)))),
				expr::Value::Null => Ok(Cbor(CborData::Null)),
				expr::Value::Bool(v) => Ok(Cbor(CborData::Bool(v))),
				expr::Value::Number(v) => match v {
					expr::Number::Int(v) => Ok(Cbor(CborData::Integer(v.into()))),
					expr::Number::Float(v) => Ok(Cbor(CborData::Float(v))),
					expr::Number::Decimal(v) => Ok(Cbor(CborData::Tag(
						TAG_STRING_DECIMAL,
						Box::new(CborData::Text(v.to_string())),
					))),
				},
				expr::Value::Strand(v) => Ok(Cbor(CborData::Text(v.0))),
				expr::Value::Duration(v) => {
					let seconds = v.secs();
					let nanos = v.subsec_nanos();

					let tag_value = match (seconds, nanos) {
						(0, 0) => Box::new(CborData::Array(vec![])),
						(_, 0) => {
							Box::new(CborData::Array(vec![CborData::Integer(seconds.into())]))
						}
						_ => Box::new(CborData::Array(vec![
							CborData::Integer(seconds.into()),
							CborData::Integer(nanos.into()),
						])),
					};

					Ok(Cbor(CborData::Tag(TAG_CUSTOM_DURATION, tag_value)))
				}
				expr::Value::Datetime(v) => {
					let seconds = v.timestamp();
					let nanos = v.timestamp_subsec_nanos();

					Ok(Cbor(CborData::Tag(
						TAG_CUSTOM_DATETIME,
						Box::new(CborData::Array(vec![
							CborData::Integer(seconds.into()),
							CborData::Integer(nanos.into()),
						])),
					)))
				}
				expr::Value::Uuid(v) => Ok(Cbor(CborData::Tag(
					TAG_SPEC_UUID,
					Box::new(CborData::Bytes(v.into_bytes().into())),
				))),
				expr::Value::Array(v) => Ok(Cbor(CborData::Array(
					v.into_iter()
						.map(|v| {
							let v = Cbor::try_from(v)?.0;
							Ok(v)
						})
						.collect::<Result<Vec<CborData>, &str>>()?,
				))),
				expr::Value::Object(v) => Ok(Cbor(CborData::Map(
					v.into_iter()
						.map(|(k, v)| {
							let k = CborData::Text(k);
							let v = Cbor::try_from(v)?.0;
							Ok((k, v))
						})
						.collect::<Result<Vec<(CborData, CborData)>, &str>>()?,
				))),
				expr::Value::Bytes(v) => Ok(Cbor(CborData::Bytes(v.into_inner()))),
				expr::Value::Thing(v) => Ok(Cbor(CborData::Tag(
					TAG_RECORDID,
					Box::new(CborData::Array(vec![
						CborData::Text(v.tb),
						match v.id {
							expr::RecordIdKeyLit::Number(v) => CborData::Integer(v.into()),
							expr::RecordIdKeyLit::String(v) => CborData::Text(v),
							expr::RecordIdKeyLit::Uuid(v) => {
								Cbor::try_from(expr::Value::from(v))?.0
							}
							expr::RecordIdKeyLit::Array(v) => {
								Cbor::try_from(expr::Value::from(v))?.0
							}
							expr::RecordIdKeyLit::Object(v) => {
								Cbor::try_from(expr::Value::from(v))?.0
							}
							expr::RecordIdKeyLit::Generate(_) => {
								return Err("Cannot encode an ungenerated Record ID into CBOR");
							}
							expr::RecordIdKeyLit::Range(v) => {
								CborData::Tag(TAG_RANGE, Box::new(CborData::try_from(*v)?))
							}
						},
					])),
				))),
				expr::Value::Table(v) => {
					Ok(Cbor(CborData::Tag(TAG_TABLE, Box::new(CborData::Text(v.0)))))
				}
				expr::Value::Geometry(v) => Ok(Cbor(encode_geometry(v)?)),
				expr::Value::Range(v) => {
					Ok(Cbor(CborData::Tag(TAG_RANGE, Box::new(CborData::try_from(*v)?))))
				}
				expr::Value::Future(v) => {
					let bin = CborData::Text(format!("{}", (*v).0));
					Ok(Cbor(CborData::Tag(TAG_FUTURE, Box::new(bin))))
				}
				expr::Value::File(expr::File {
					bucket,
					key,
				}) => Ok(Cbor(CborData::Tag(
					TAG_FILE,
					Box::new(CborData::Array(vec![CborData::Text(bucket), CborData::Text(key)])),
				))),
				// We shouldn't reach here
				_ => Err("Found unsupported SurrealQL value being encoded into a CBOR value"),
			}
		}
	}

	fn encode_geometry(v: expr::Geometry) -> Result<CborData, &'static str> {
		match v {
			expr::Geometry::Point(v) => Ok(CborData::Tag(
				TAG_GEOMETRY_POINT,
				Box::new(CborData::Array(vec![CborData::Float(v.x()), CborData::Float(v.y())])),
			)),
			expr::Geometry::Line(v) => {
				let data = v
					.points()
					.map(|v| encode_geometry(v.into()))
					.collect::<Result<Vec<CborData>, &'static str>>()?;

				Ok(CborData::Tag(TAG_GEOMETRY_LINE, Box::new(CborData::Array(data))))
			}
			expr::Geometry::Polygon(v) => {
				let data = once(v.exterior())
					.chain(v.interiors())
					.map(|v| encode_geometry(v.clone().into()))
					.collect::<Result<Vec<CborData>, &'static str>>()?;

				Ok(CborData::Tag(TAG_GEOMETRY_POLYGON, Box::new(CborData::Array(data))))
			}
			expr::Geometry::MultiPoint(v) => {
				let data = v
					.iter()
					.map(|v| encode_geometry((*v).into()))
					.collect::<Result<Vec<CborData>, &'static str>>()?;

				Ok(CborData::Tag(TAG_GEOMETRY_MULTIPOINT, Box::new(CborData::Array(data))))
			}
			expr::Geometry::MultiLine(v) => {
				let data = v
					.iter()
					.map(|v| encode_geometry(v.clone().into()))
					.collect::<Result<Vec<CborData>, &'static str>>()?;

				Ok(CborData::Tag(TAG_GEOMETRY_MULTILINE, Box::new(CborData::Array(data))))
			}
			expr::Geometry::MultiPolygon(v) => {
				let data = v
					.iter()
					.map(|v| encode_geometry(v.clone().into()))
					.collect::<Result<Vec<CborData>, &'static str>>()?;

				Ok(CborData::Tag(TAG_GEOMETRY_MULTIPOLYGON, Box::new(CborData::Array(data))))
			}
			expr::Geometry::Collection(v) => {
				let data = v
					.iter()
					.map(|v| encode_geometry(v.clone()))
					.collect::<Result<Vec<CborData>, &'static str>>()?;

				Ok(CborData::Tag(TAG_GEOMETRY_COLLECTION, Box::new(CborData::Array(data))))
			}
		}
	}

	impl TryFrom<CborData> for expr::Range {
		type Error = &'static str;
		fn try_from(val: CborData) -> Result<Self, &'static str> {
			fn decode_bound(v: CborData) -> Result<Bound<expr::Value>, &'static str> {
				match v {
					CborData::Tag(TAG_BOUND_INCLUDED, v) => {
						Ok(Bound::Included(expr::Value::try_from(Cbor(*v))?))
					}
					CborData::Tag(TAG_BOUND_EXCLUDED, v) => {
						Ok(Bound::Excluded(expr::Value::try_from(Cbor(*v))?))
					}
					CborData::Null => Ok(Bound::Unbounded),
					_ => Err("Expected a bound tag"),
				}
			}

			match val {
				CborData::Array(v) if v.len() == 2 => {
					let mut v = v;
					let beg = decode_bound(v.remove(0).clone())?;
					let end = decode_bound(v.remove(0).clone())?;
					Ok(expr::Range::new(beg, end))
				}
				_ => Err("Expected a CBOR array with 2 bounds"),
			}
		}
	}

	impl TryFrom<expr::Range> for CborData {
		type Error = &'static str;
		fn try_from(r: expr::Range) -> Result<CborData, &'static str> {
			fn encode(b: Bound<expr::Value>) -> Result<CborData, &'static str> {
				match b {
					Bound::Included(v) => {
						Ok(CborData::Tag(TAG_BOUND_INCLUDED, Box::new(Cbor::try_from(v)?.0)))
					}
					Bound::Excluded(v) => {
						Ok(CborData::Tag(TAG_BOUND_EXCLUDED, Box::new(Cbor::try_from(v)?.0)))
					}
					Bound::Unbounded => Ok(CborData::Null),
				}
			}

			Ok(CborData::Array(vec![encode(r.beg)?, encode(r.end)?]))
		}
	}

	impl TryFrom<CborData> for expr::id::range::RecordIdKeyRangeLit {
		type Error = &'static str;
		fn try_from(val: CborData) -> Result<Self, &'static str> {
			fn decode_bound(v: CborData) -> Result<Bound<expr::RecordIdKeyLit>, &'static str> {
				match v {
					CborData::Tag(TAG_BOUND_INCLUDED, v) => {
						Ok(Bound::Included(expr::RecordIdKeyLit::try_from(*v)?))
					}
					CborData::Tag(TAG_BOUND_EXCLUDED, v) => {
						Ok(Bound::Excluded(expr::RecordIdKeyLit::try_from(*v)?))
					}
					CborData::Null => Ok(Bound::Unbounded),
					_ => Err("Expected a bound tag"),
				}
			}

			match val {
				CborData::Array(v) if v.len() == 2 => {
					let mut v = v;
					let beg = decode_bound(v.remove(0).clone())?;
					let end = decode_bound(v.remove(0).clone())?;
					Ok(expr::id::range::RecordIdKeyRangeLit::try_from((beg, end))
						.map_err(|_| "Found an invalid range with ranges as bounds")?)
				}
				_ => Err("Expected a CBOR array with 2 bounds"),
			}
		}
	}

	impl TryFrom<expr::id::range::RecordIdKeyRangeLit> for CborData {
		type Error = &'static str;
		fn try_from(r: expr::id::range::RecordIdKeyRangeLit) -> Result<CborData, &'static str> {
			fn encode(b: Bound<expr::RecordIdKeyLit>) -> Result<CborData, &'static str> {
				match b {
					Bound::Included(v) => {
						Ok(CborData::Tag(TAG_BOUND_INCLUDED, Box::new(v.try_into()?)))
					}
					Bound::Excluded(v) => {
						Ok(CborData::Tag(TAG_BOUND_EXCLUDED, Box::new(v.try_into()?)))
					}
					Bound::Unbounded => Ok(CborData::Null),
				}
			}

			Ok(CborData::Array(vec![encode(r.start)?, encode(r.end)?]))
		}
	}

	impl TryFrom<CborData> for expr::RecordIdKeyLit {
		type Error = &'static str;
		fn try_from(val: CborData) -> Result<Self, &'static str> {
			match val {
				CborData::Integer(v) => Ok(expr::RecordIdKeyLit::Number(i128::from(v) as i64)),
				CborData::Text(v) => Ok(expr::RecordIdKeyLit::String(v)),
				CborData::Array(v) => Ok(expr::RecordIdKeyLit::Array(v.try_into()?)),
				CborData::Map(v) => Ok(expr::RecordIdKeyLit::Object(v.try_into()?)),
				CborData::Tag(TAG_RANGE, v) => Ok(expr::RecordIdKeyLit::Range(Box::new(
					expr::id::range::RecordIdKeyRangeLit::try_from(*v)?,
				))),
				CborData::Tag(TAG_STRING_UUID, v) => {
					v.deref().to_owned().try_into().map(expr::RecordIdKeyLit::Uuid)
				}
				CborData::Tag(TAG_SPEC_UUID, v) => {
					v.deref().to_owned().try_into().map(expr::RecordIdKeyLit::Uuid)
				}
				_ => Err("Expected a CBOR integer, text, array or map"),
			}
		}
	}

	impl TryFrom<expr::RecordIdKeyLit> for CborData {
		type Error = &'static str;
		fn try_from(v: expr::RecordIdKeyLit) -> Result<CborData, &'static str> {
			match v {
				expr::RecordIdKeyLit::Number(v) => Ok(CborData::Integer(v.into())),
				expr::RecordIdKeyLit::String(v) => Ok(CborData::Text(v)),
				expr::RecordIdKeyLit::Array(v) => Ok(Cbor::try_from(expr::Value::from(v))?.0),
				expr::RecordIdKeyLit::Object(v) => Ok(Cbor::try_from(expr::Value::from(v))?.0),
				expr::RecordIdKeyLit::Range(v) => {
					Ok(CborData::Tag(TAG_RANGE, Box::new(v.deref().to_owned().try_into()?)))
				}
				expr::RecordIdKeyLit::Uuid(v) => Ok(CborData::Tag(
					TAG_SPEC_UUID,
					Box::new(CborData::Bytes(v.into_bytes().into())),
				)),
				expr::RecordIdKeyLit::Generate(_) => {
					Err("Cannot encode an ungenerated Record ID into CBOR")
				}
			}
		}
	}

	impl TryFrom<Vec<CborData>> for expr::Array {
		type Error = &'static str;
		fn try_from(val: Vec<CborData>) -> Result<Self, &'static str> {
			val.into_iter()
				.map(|v| expr::Value::try_from(Cbor(v)))
				.collect::<Result<expr::Array, &str>>()
		}
	}

	impl TryFrom<Vec<(CborData, CborData)>> for expr::Object {
		type Error = &'static str;
		fn try_from(val: Vec<(CborData, CborData)>) -> Result<Self, &'static str> {
			Ok(expr::Object(
				val.into_iter()
					.map(|(k, v)| {
						let k = expr::Value::try_from(Cbor(k)).map(|k| k.as_raw_string());
						let v = expr::Value::try_from(Cbor(v));
						Ok((k?, v?))
					})
					.collect::<Result<BTreeMap<String, expr::Value>, &str>>()?,
			))
		}
	}

	impl TryFrom<CborData> for expr::Uuid {
		type Error = &'static str;
		fn try_from(val: CborData) -> Result<Self, &'static str> {
			match val {
				CborData::Bytes(v) if v.len() == 16 => match v.as_slice().try_into() {
					Ok(v) => Ok(expr::Uuid::from(uuid::Uuid::from_bytes(v))),
					Err(_) => Err("Expected a CBOR byte array with 16 elements"),
				},
				_ => Err("Expected a CBOR byte array with 16 elements"),
			}
		}
	}
}
