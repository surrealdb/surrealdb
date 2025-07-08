use crate::rpc::protocol::v1::types::V1IdRange;
use crate::rpc::protocol::v1::types::{
	V1Array, V1Datetime, V1Duration, V1File, V1Geometry, V1Id, V1Number, V1Object, V1RecordId,
	V1Uuid, V1Value,
};
use crate::sql::number::decimal::DecimalExt;
use ciborium::Value as CborData;
use geo::{LineString, Point, Polygon};
use geo_types::{MultiLineString, MultiPoint, MultiPolygon};
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::iter::once;
use std::ops::Bound;
use std::ops::Deref;

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
#[allow(dead_code)]
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

impl TryFrom<Cbor> for V1Value {
	type Error = &'static str;
	fn try_from(val: Cbor) -> Result<Self, &'static str> {
		match val.0 {
			CborData::Null => Ok(V1Value::Null),
			CborData::Bool(v) => Ok(V1Value::from(v)),
			CborData::Integer(v) => Ok(V1Value::from(i128::from(v))),
			CborData::Float(v) => Ok(V1Value::from(v)),
			CborData::Bytes(v) => Ok(V1Value::Bytes(v.into())),
			CborData::Text(v) => Ok(V1Value::from(v)),
			CborData::Array(v) => Ok(V1Value::Array(V1Array::try_from(v)?)),
			CborData::Map(v) => Ok(V1Value::Object(V1Object::try_from(v)?)),
			CborData::Tag(t, v) => {
				match t {
					// A literal datetime
					TAG_SPEC_DATETIME => match *v {
						CborData::Text(v) => match V1Datetime::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid V1Datetime value"),
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

							match V1Datetime::try_from((seconds, nanos)) {
								Ok(v) => Ok(v.into()),
								_ => Err("Expected a valid V1Datetime value"),
							}
						}
						_ => Err("Expected a CBOR array with 2 elements"),
					},
					// A literal NONE
					TAG_NONE => Ok(V1Value::None),
					// A literal uuid
					TAG_STRING_UUID => match *v {
						CborData::Text(v) => match V1Uuid::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid UUID value"),
						},
						_ => Err("Expected a CBOR text data type"),
					},
					// A byte string uuid
					TAG_SPEC_UUID => v.deref().to_owned().try_into().map(V1Value::Uuid),
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
						CborData::Text(v) => match V1Duration::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid V1Duration value"),
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

							Ok(V1Duration::new(seconds as i64, nanos).into())
						}
						_ => Err("Expected a CBOR array with at most 2 elements"),
					},
					// A literal recordid
					TAG_RECORDID => match *v {
						CborData::Text(v) => match V1RecordId::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid RecordID value"),
						},
						CborData::Array(mut v) if v.len() == 2 => {
							let tb = match V1Value::try_from(Cbor(v.remove(0))) {
								Ok(V1Value::Strand(tb)) => tb.0,
								Ok(V1Value::Table(tb)) => tb.0,
								_ => {
									return Err(
										"Expected the tb of a Record Id to be a String or Table value",
									);
								}
							};

							let id = V1Id::try_from(v.remove(0))?;

							Ok(V1Value::RecordId(V1RecordId {
								tb,
								id,
							}))
						}
						_ => Err("Expected a CBOR text data type, or a CBOR array with 2 elements"),
					},
					// A literal table
					TAG_TABLE => match *v {
						CborData::Text(v) => Ok(V1Value::Table(v.into())),
						_ => Err("Expected a CBOR text data type"),
					},
					TAG_GEOMETRY_POINT => match *v {
						CborData::Array(mut v) if v.len() == 2 => {
							let x = V1Value::try_from(Cbor(v.remove(0)))?;
							let y = V1Value::try_from(Cbor(v.remove(0)))?;

							match (x, y) {
								(V1Value::Number(x), V1Value::Number(y)) => Ok(V1Value::Geometry(
									V1Geometry::Point((x.as_float(), y.as_float()).into()),
								)),
								_ => Err("Expected a CBOR array with 2 decimal values"),
							}
						}
						_ => Err("Expected a CBOR array with 2 decimal values"),
					},
					TAG_GEOMETRY_LINE => match v.deref() {
						CborData::Array(v) => {
							let points = v
								.iter()
								.map(|v| match V1Value::try_from(Cbor(v.clone()))? {
									V1Value::Geometry(V1Geometry::Point(v)) => Ok(v),
									_ => Err("Expected a CBOR array with Geometry Point values"),
								})
								.collect::<Result<Vec<Point>, &str>>()?;

							Ok(V1Value::Geometry(V1Geometry::Line(LineString::from(points))))
						}
						_ => Err("Expected a CBOR array with Geometry Point values"),
					},
					TAG_GEOMETRY_POLYGON => match v.deref() {
						CborData::Array(v) if !v.is_empty() => {
							let lines = v
								.iter()
								.map(|v| match V1Value::try_from(Cbor(v.clone()))? {
									V1Value::Geometry(V1Geometry::Line(v)) => Ok(v),
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

							Ok(V1Value::Geometry(V1Geometry::Polygon(Polygon::new(
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
								.map(|v| match V1Value::try_from(Cbor(v.clone()))? {
									V1Value::Geometry(V1Geometry::Point(v)) => Ok(v),
									_ => Err("Expected a CBOR array with Geometry Point values"),
								})
								.collect::<Result<Vec<Point>, &str>>()?;

							Ok(V1Value::Geometry(V1Geometry::MultiPoint(MultiPoint::from(points))))
						}
						_ => Err("Expected a CBOR array with Geometry Point values"),
					},
					TAG_GEOMETRY_MULTILINE => match v.deref() {
						CborData::Array(v) => {
							let lines = v
								.iter()
								.map(|v| match V1Value::try_from(Cbor(v.clone()))? {
									V1Value::Geometry(V1Geometry::Line(v)) => Ok(v),
									_ => Err("Expected a CBOR array with Geometry Line values"),
								})
								.collect::<Result<Vec<LineString>, &str>>()?;

							Ok(V1Value::Geometry(V1Geometry::MultiLine(MultiLineString::new(
								lines,
							))))
						}
						_ => Err("Expected a CBOR array with Geometry Line values"),
					},
					TAG_GEOMETRY_MULTIPOLYGON => match v.deref() {
						CborData::Array(v) => {
							let polygons = v
								.iter()
								.map(|v| match V1Value::try_from(Cbor(v.clone()))? {
									V1Value::Geometry(V1Geometry::Polygon(v)) => Ok(v),
									_ => Err("Expected a CBOR array with Geometry Polygon values"),
								})
								.collect::<Result<Vec<Polygon>, &str>>()?;

							Ok(V1Value::Geometry(V1Geometry::MultiPolygon(MultiPolygon::from(
								polygons,
							))))
						}
						_ => Err("Expected a CBOR array with Geometry Polygon values"),
					},
					TAG_GEOMETRY_COLLECTION => match v.deref() {
						CborData::Array(v) => {
							let geometries = v
								.iter()
								.map(|v| match V1Value::try_from(Cbor(v.clone()))? {
									V1Value::Geometry(v) => Ok(v),
									_ => Err("Expected a CBOR array with Geometry values"),
								})
								.collect::<Result<Vec<V1Geometry>, &str>>()?;

							Ok(V1Value::Geometry(V1Geometry::Collection(geometries)))
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

							Ok(V1Value::File(V1File {
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

impl TryFrom<V1Value> for Cbor {
	type Error = &'static str;
	fn try_from(val: V1Value) -> Result<Self, &'static str> {
		match val {
			V1Value::None => Ok(Cbor(CborData::Tag(TAG_NONE, Box::new(CborData::Null)))),
			V1Value::Null => Ok(Cbor(CborData::Null)),
			V1Value::Bool(v) => Ok(Cbor(CborData::Bool(v))),
			V1Value::Number(v) => match v {
				V1Number::Int(v) => Ok(Cbor(CborData::Integer(v.into()))),
				V1Number::Float(v) => Ok(Cbor(CborData::Float(v))),
				V1Number::Decimal(v) => Ok(Cbor(CborData::Tag(
					TAG_STRING_DECIMAL,
					Box::new(CborData::Text(v.to_string())),
				))),
			},
			V1Value::Strand(v) => Ok(Cbor(CborData::Text(v.0))),
			V1Value::Duration(v) => {
				let seconds = v.0.as_secs();
				let nanos = v.0.subsec_nanos();

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
			V1Value::Datetime(v) => {
				let seconds = v.0.timestamp();
				let nanos = v.0.timestamp_subsec_nanos();

				Ok(Cbor(CborData::Tag(
					TAG_CUSTOM_DATETIME,
					Box::new(CborData::Array(vec![
						CborData::Integer(seconds.into()),
						CborData::Integer(nanos.into()),
					])),
				)))
			}
			V1Value::Uuid(v) => Ok(Cbor(CborData::Tag(
				TAG_SPEC_UUID,
				Box::new(CborData::Bytes(v.0.into_bytes().into())),
			))),
			V1Value::Array(v) => Ok(Cbor(CborData::Array(
				v.0.into_iter()
					.map(|v| {
						let v = Cbor::try_from(v)?.0;
						Ok(v)
					})
					.collect::<Result<Vec<CborData>, &str>>()?,
			))),
			V1Value::Object(v) => Ok(Cbor(CborData::Map(
				v.into_iter()
					.map(|(k, v)| {
						let k = CborData::Text(k);
						let v = Cbor::try_from(v)?.0;
						Ok((k, v))
					})
					.collect::<Result<Vec<(CborData, CborData)>, &str>>()?,
			))),
			V1Value::Bytes(v) => Ok(Cbor(CborData::Bytes(v.0))),
			V1Value::RecordId(v) => Ok(Cbor(CborData::Tag(
				TAG_RECORDID,
				Box::new(CborData::Array(vec![
					CborData::Text(v.tb),
					match v.id {
						V1Id::Number(v) => CborData::Integer(v.into()),
						V1Id::String(v) => CborData::Text(v),
						V1Id::Uuid(v) => Cbor::try_from(V1Value::from(v))?.0,
						V1Id::Array(v) => Cbor::try_from(V1Value::from(v))?.0,
						V1Id::Object(v) => Cbor::try_from(V1Value::from(v))?.0,
						V1Id::Generate(_) => {
							return Err("Cannot encode an ungenerated Record ID into CBOR");
						}
						V1Id::Range(v) => {
							CborData::Tag(TAG_RANGE, Box::new(CborData::try_from(*v)?))
						}
					},
				])),
			))),
			V1Value::Table(v) => Ok(Cbor(CborData::Tag(TAG_TABLE, Box::new(CborData::Text(v.0))))),
			V1Value::Geometry(v) => Ok(Cbor(encode_geometry(v)?)),
			V1Value::File(V1File {
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

fn encode_geometry(v: V1Geometry) -> Result<CborData, &'static str> {
	match v {
		V1Geometry::Point(v) => Ok(CborData::Tag(
			TAG_GEOMETRY_POINT,
			Box::new(CborData::Array(vec![CborData::Float(v.x()), CborData::Float(v.y())])),
		)),
		V1Geometry::Line(v) => {
			let data = v
				.points()
				.map(|v| encode_geometry(v.into()))
				.collect::<Result<Vec<CborData>, &'static str>>()?;

			Ok(CborData::Tag(TAG_GEOMETRY_LINE, Box::new(CborData::Array(data))))
		}
		V1Geometry::Polygon(v) => {
			let data = once(v.exterior())
				.chain(v.interiors())
				.map(|v| encode_geometry(v.clone().into()))
				.collect::<Result<Vec<CborData>, &'static str>>()?;

			Ok(CborData::Tag(TAG_GEOMETRY_POLYGON, Box::new(CborData::Array(data))))
		}
		V1Geometry::MultiPoint(v) => {
			let data = v
				.iter()
				.map(|v| encode_geometry((*v).into()))
				.collect::<Result<Vec<CborData>, &'static str>>()?;

			Ok(CborData::Tag(TAG_GEOMETRY_MULTIPOINT, Box::new(CborData::Array(data))))
		}
		V1Geometry::MultiLine(v) => {
			let data = v
				.iter()
				.map(|v| encode_geometry(v.clone().into()))
				.collect::<Result<Vec<CborData>, &'static str>>()?;

			Ok(CborData::Tag(TAG_GEOMETRY_MULTILINE, Box::new(CborData::Array(data))))
		}
		V1Geometry::MultiPolygon(v) => {
			let data = v
				.iter()
				.map(|v| encode_geometry(v.clone().into()))
				.collect::<Result<Vec<CborData>, &'static str>>()?;

			Ok(CborData::Tag(TAG_GEOMETRY_MULTIPOLYGON, Box::new(CborData::Array(data))))
		}
		V1Geometry::Collection(v) => {
			let data = v
				.iter()
				.map(|v| encode_geometry(v.clone()))
				.collect::<Result<Vec<CborData>, &'static str>>()?;

			Ok(CborData::Tag(TAG_GEOMETRY_COLLECTION, Box::new(CborData::Array(data))))
		}
	}
}

impl TryFrom<CborData> for V1IdRange {
	type Error = &'static str;
	fn try_from(val: CborData) -> Result<Self, &'static str> {
		fn decode_bound(v: CborData) -> Result<Bound<V1Id>, &'static str> {
			match v {
				CborData::Tag(TAG_BOUND_INCLUDED, v) => Ok(Bound::Included(V1Id::try_from(*v)?)),
				CborData::Tag(TAG_BOUND_EXCLUDED, v) => Ok(Bound::Excluded(V1Id::try_from(*v)?)),
				CborData::Null => Ok(Bound::Unbounded),
				_ => Err("Expected a bound tag"),
			}
		}

		match val {
			CborData::Array(v) if v.len() == 2 => {
				let mut v = v;
				let beg = decode_bound(v.remove(0).clone())?;
				let end = decode_bound(v.remove(0).clone())?;
				Ok(V1IdRange::from((beg, end)))
			}
			_ => Err("Expected a CBOR array with 2 bounds"),
		}
	}
}

impl TryFrom<V1IdRange> for CborData {
	type Error = &'static str;
	fn try_from(r: V1IdRange) -> Result<CborData, &'static str> {
		fn encode(b: Bound<V1Id>) -> Result<CborData, &'static str> {
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

		Ok(CborData::Array(vec![encode(r.beg)?, encode(r.end)?]))
	}
}

impl TryFrom<CborData> for V1Id {
	type Error = &'static str;
	fn try_from(val: CborData) -> Result<Self, &'static str> {
		match val {
			CborData::Integer(v) => Ok(V1Id::Number(i128::from(v) as i64)),
			CborData::Text(v) => Ok(V1Id::String(v)),
			CborData::Array(v) => Ok(V1Id::Array(v.try_into()?)),
			CborData::Map(v) => Ok(V1Id::Object(v.try_into()?)),
			CborData::Tag(TAG_RANGE, v) => Ok(V1Id::Range(Box::new(V1IdRange::try_from(*v)?))),
			CborData::Tag(TAG_STRING_UUID, v) => v.deref().to_owned().try_into().map(V1Id::Uuid),
			CborData::Tag(TAG_SPEC_UUID, v) => v.deref().to_owned().try_into().map(V1Id::Uuid),
			_ => Err("Expected a CBOR integer, text, array or map"),
		}
	}
}

impl TryFrom<V1Id> for CborData {
	type Error = &'static str;
	fn try_from(v: V1Id) -> Result<CborData, &'static str> {
		match v {
			V1Id::Number(v) => Ok(CborData::Integer(v.into())),
			V1Id::String(v) => Ok(CborData::Text(v)),
			V1Id::Array(v) => Ok(Cbor::try_from(V1Value::from(v))?.0),
			V1Id::Object(v) => Ok(Cbor::try_from(V1Value::from(v))?.0),
			V1Id::Range(v) => {
				Ok(CborData::Tag(TAG_RANGE, Box::new(v.deref().to_owned().try_into()?)))
			}
			V1Id::Uuid(v) => {
				Ok(CborData::Tag(TAG_SPEC_UUID, Box::new(CborData::Bytes(v.0.into_bytes().into()))))
			}
			V1Id::Generate(_) => Err("Cannot encode an ungenerated Record ID into CBOR"),
		}
	}
}

impl TryFrom<Vec<CborData>> for V1Array {
	type Error = &'static str;
	fn try_from(val: Vec<CborData>) -> Result<Self, &'static str> {
		val.into_iter().map(|v| V1Value::try_from(Cbor(v))).collect::<Result<V1Array, &str>>()
	}
}

impl TryFrom<Vec<(CborData, CborData)>> for V1Object {
	type Error = &'static str;
	fn try_from(val: Vec<(CborData, CborData)>) -> Result<Self, &'static str> {
		Ok(V1Object(
			val.into_iter()
				.map(|(k, v)| {
					let k = V1Value::try_from(Cbor(k)).map(|k| k.as_string());
					let v = V1Value::try_from(Cbor(v));
					Ok((k?, v?))
				})
				.collect::<Result<BTreeMap<String, V1Value>, &str>>()?,
		))
	}
}

impl TryFrom<CborData> for V1Uuid {
	type Error = &'static str;
	fn try_from(val: CborData) -> Result<Self, &'static str> {
		match val {
			CborData::Bytes(v) if v.len() == 16 => match v.as_slice().try_into() {
				Ok(v) => Ok(V1Uuid::from(uuid::Uuid::from_bytes(v))),
				Err(_) => Err("Expected a CBOR byte array with 16 elements"),
			},
			_ => Err("Expected a CBOR byte array with 16 elements"),
		}
	}
}
