use crate::rpc::protocol::v1::types::V1IdRange;
use crate::rpc::protocol::v1::types::{
	V1Array, V1Datetime, V1Duration, V1File, V1Geometry, V1Id, V1Number, V1Object, V1RecordId,
	V1Uuid, V1Value,
};
use crate::sql::number::decimal::DecimalExt;
use ciborium::Value as CborValue;
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

impl V1Value {
	pub fn from_cbor(val: CborValue) -> anyhow::Result<Self> {
		match val {
			CborValue::Null => Ok(V1Value::Null),
			CborValue::Bool(v) => Ok(V1Value::from(v)),
			CborValue::Integer(v) => Ok(V1Value::from(i128::from(v))),
			CborValue::Float(v) => Ok(V1Value::from(v)),
			CborValue::Bytes(v) => Ok(V1Value::Bytes(v.into())),
			CborValue::Text(v) => Ok(V1Value::from(v)),
			CborValue::Array(v) => Ok(V1Value::Array(V1Array::from_cbor(v)?)),
			CborValue::Map(v) => Ok(V1Value::Object(V1Object::from_cbor(v)?)),
			CborValue::Tag(t, v) => {
				match t {
					// A literal datetime
					TAG_SPEC_DATETIME => match *v {
						CborValue::Text(v) => match V1Datetime::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => anyhow::bail!("Expected a valid V1Datetime value"),
						},
						_ => anyhow::bail!("Expected a CBOR text data type"),
					},
					// A custom [seconds: i64, nanos: u32] datetime
					TAG_CUSTOM_DATETIME => match *v {
						CborValue::Array(v) if v.len() == 2 => {
							let mut iter = v.into_iter();

							let seconds = match iter.next() {
								Some(CborValue::Integer(v)) => match i64::try_from(v) {
									Ok(v) => v,
									_ => anyhow::bail!("Expected a CBOR integer data type"),
								},
								_ => anyhow::bail!("Expected a CBOR integer data type"),
							};

							let nanos = match iter.next() {
								Some(CborValue::Integer(v)) => match u32::try_from(v) {
									Ok(v) => v,
									_ => anyhow::bail!("Expected a CBOR integer data type"),
								},
								_ => anyhow::bail!("Expected a CBOR integer data type"),
							};

							match V1Datetime::try_from((seconds, nanos)) {
								Ok(v) => Ok(v.into()),
								_ => anyhow::bail!("Expected a valid V1Datetime value"),
							}
						}
						_ => anyhow::bail!("Expected a CBOR array with 2 elements"),
					},
					// A literal NONE
					TAG_NONE => Ok(V1Value::None),
					// A literal uuid
					TAG_STRING_UUID => match *v {
						CborValue::Text(v) => match V1Uuid::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => anyhow::bail!("Expected a valid UUID value"),
						},
						_ => anyhow::bail!("Expected a CBOR text data type"),
					},
					// A byte string uuid
					TAG_SPEC_UUID => V1Uuid::from_cbor(v.deref().to_owned()).map(V1Value::Uuid),
					// A literal decimal
					TAG_STRING_DECIMAL => match *v {
						CborValue::Text(v) => match Decimal::from_str_normalized(v.as_str()) {
							Ok(v) => Ok(v.into()),
							_ => anyhow::bail!("Expected a valid Decimal value"),
						},
						_ => anyhow::bail!("Expected a CBOR text data type"),
					},
					// A literal duration
					TAG_STRING_DURATION => match *v {
						CborValue::Text(v) => match V1Duration::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => anyhow::bail!("Expected a valid V1Duration value"),
						},
						_ => anyhow::bail!("Expected a CBOR text data type"),
					},
					// A custom [seconds: Option<u64>, nanos: Option<u32>] duration
					TAG_CUSTOM_DURATION => match *v {
						CborValue::Array(v) if v.len() <= 2 => {
							let mut iter = v.into_iter();

							let seconds = match iter.next() {
								Some(CborValue::Integer(v)) => match u64::try_from(v) {
									Ok(v) => v,
									_ => anyhow::bail!("Expected a CBOR integer data type"),
								},
								_ => 0,
							};

							let nanos = match iter.next() {
								Some(CborValue::Integer(v)) => match u32::try_from(v) {
									Ok(v) => v,
									_ => anyhow::bail!("Expected a CBOR integer data type"),
								},
								_ => 0,
							};

							Ok(V1Duration::new(seconds as i64, nanos).into())
						}
						_ => anyhow::bail!("Expected a CBOR array with at most 2 elements"),
					},
					// A literal recordid
					TAG_RECORDID => match *v {
						CborValue::Text(v) => match V1RecordId::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => anyhow::bail!("Expected a valid RecordID value"),
						},
						CborValue::Array(mut v) if v.len() == 2 => {
							let tb = match V1Value::from_cbor(v.remove(0)) {
								Ok(V1Value::Strand(tb)) => tb.0,
								Ok(V1Value::Table(tb)) => tb.0,
								_ => {
									anyhow::bail!(
										"Expected the tb of a Record Id to be a String or Table value"
									);
								}
							};

							let id = V1Id::from_cbor(v.remove(0))?;

							Ok(V1Value::RecordId(V1RecordId {
								tb,
								id,
							}))
						}
						_ => anyhow::bail!(
							"Expected a CBOR text data type, or a CBOR array with 2 elements"
						),
					},
					// A literal table
					TAG_TABLE => match *v {
						CborValue::Text(v) => Ok(V1Value::Table(v.into())),
						_ => anyhow::bail!("Expected a CBOR text data type"),
					},
					TAG_GEOMETRY_POINT => match *v {
						CborValue::Array(mut v) if v.len() == 2 => {
							let x = V1Value::from_cbor(v.remove(0))?;
							let y = V1Value::from_cbor(v.remove(0))?;

							match (x, y) {
								(V1Value::Number(x), V1Value::Number(y)) => Ok(V1Value::Geometry(
									V1Geometry::Point((x.as_float(), y.as_float()).into()),
								)),
								_ => anyhow::bail!("Expected a CBOR array with 2 decimal values"),
							}
						}
						_ => anyhow::bail!("Expected a CBOR array with 2 decimal values"),
					},
					TAG_GEOMETRY_LINE => match v.deref() {
						CborValue::Array(v) => {
							let points = v
								.iter()
								.map(|v| match V1Value::from_cbor(v.clone())? {
									V1Value::Geometry(V1Geometry::Point(v)) => Ok(v),
									_ => anyhow::bail!(
										"Expected a CBOR array with Geometry Point values"
									),
								})
								.collect::<Result<Vec<Point>, anyhow::Error>>()?;

							Ok(V1Value::Geometry(V1Geometry::Line(LineString::from(points))))
						}
						_ => anyhow::bail!("Expected a CBOR array with Geometry Point values"),
					},
					TAG_GEOMETRY_POLYGON => match v.deref() {
						CborValue::Array(v) if !v.is_empty() => {
							let lines = v
								.iter()
								.map(|v| match V1Value::from_cbor(v.clone())? {
									V1Value::Geometry(V1Geometry::Line(v)) => Ok(v),
									_ => anyhow::bail!(
										"Expected a CBOR array with Geometry Line values"
									),
								})
								.collect::<Result<Vec<LineString>, anyhow::Error>>()?;

							let exterior = match lines.first() {
								Some(v) => v,
								_ => {
									anyhow::bail!(
										"Expected a CBOR array with at least one Geometry Line values"
									);
								}
							};
							let interiors = Vec::from(&lines[1..]);

							Ok(V1Value::Geometry(V1Geometry::Polygon(Polygon::new(
								exterior.clone(),
								interiors,
							))))
						}
						_ => anyhow::bail!(
							"Expected a CBOR array with at least one Geometry Line values"
						),
					},
					TAG_GEOMETRY_MULTIPOINT => match v.deref() {
						CborValue::Array(v) => {
							let points = v
								.iter()
								.map(|v| match V1Value::from_cbor(v.clone())? {
									V1Value::Geometry(V1Geometry::Point(v)) => Ok(v),
									_ => anyhow::bail!(
										"Expected a CBOR array with Geometry Point values"
									),
								})
								.collect::<Result<Vec<Point>, anyhow::Error>>()?;

							Ok(V1Value::Geometry(V1Geometry::MultiPoint(MultiPoint::from(points))))
						}
						_ => anyhow::bail!("Expected a CBOR array with Geometry Point values"),
					},
					TAG_GEOMETRY_MULTILINE => match v.deref() {
						CborValue::Array(v) => {
							let lines = v
								.iter()
								.map(|v| match V1Value::from_cbor(v.clone())? {
									V1Value::Geometry(V1Geometry::Line(v)) => Ok(v),
									_ => anyhow::bail!(
										"Expected a CBOR array with Geometry Line values"
									),
								})
								.collect::<Result<Vec<LineString>, anyhow::Error>>()?;

							Ok(V1Value::Geometry(V1Geometry::MultiLine(MultiLineString::new(
								lines,
							))))
						}
						_ => anyhow::bail!("Expected a CBOR array with Geometry Line values"),
					},
					TAG_GEOMETRY_MULTIPOLYGON => match v.deref() {
						CborValue::Array(v) => {
							let polygons = v
								.iter()
								.map(|v| match V1Value::from_cbor(v.clone())? {
									V1Value::Geometry(V1Geometry::Polygon(v)) => Ok(v),
									_ => anyhow::bail!(
										"Expected a CBOR array with Geometry Polygon values"
									),
								})
								.collect::<Result<Vec<Polygon>, anyhow::Error>>()?;

							Ok(V1Value::Geometry(V1Geometry::MultiPolygon(MultiPolygon::from(
								polygons,
							))))
						}
						_ => anyhow::bail!("Expected a CBOR array with Geometry Polygon values"),
					},
					TAG_GEOMETRY_COLLECTION => match v.deref() {
						CborValue::Array(v) => {
							let geometries = v
								.iter()
								.map(|v| match V1Value::from_cbor(v.clone())? {
									V1Value::Geometry(v) => Ok(v),
									_ => {
										anyhow::bail!("Expected a CBOR array with Geometry values")
									}
								})
								.collect::<Result<Vec<V1Geometry>, anyhow::Error>>()?;

							Ok(V1Value::Geometry(V1Geometry::Collection(geometries)))
						}
						_ => anyhow::bail!("Expected a CBOR array with Geometry values"),
					},
					TAG_FILE => match *v {
						CborValue::Array(mut v) if v.len() == 2 => {
							let CborValue::Text(bucket) = v.remove(0) else {
								anyhow::bail!("Expected the bucket name to be a string value");
							};

							let CborValue::Text(key) = v.remove(0) else {
								anyhow::bail!("Expected the file key to be a string value");
							};

							Ok(V1Value::File(V1File {
								bucket,
								key,
							}))
						}
						_ => anyhow::bail!(
							"Expected a CBOR array with two String bucket and key values"
						),
					},
					// An unknown tag
					_ => anyhow::bail!("Encountered an unknown CBOR tag"),
				}
			}
			_ => anyhow::bail!("Encountered an unknown CBOR data type"),
		}
	}

	pub fn into_cbor(self) -> anyhow::Result<CborValue> {
		match self {
			V1Value::None => Ok(CborValue::Tag(TAG_NONE, Box::new(CborValue::Null))),
			V1Value::Null => Ok(CborValue::Null),
			V1Value::Bool(v) => Ok(CborValue::Bool(v)),
			V1Value::Number(v) => match v {
				V1Number::Int(v) => Ok(CborValue::Integer(v.into())),
				V1Number::Float(v) => Ok(CborValue::Float(v)),
				V1Number::Decimal(v) => {
					Ok(CborValue::Tag(TAG_STRING_DECIMAL, Box::new(CborValue::Text(v.to_string()))))
				}
			},
			V1Value::Strand(v) => Ok(CborValue::Text(v.0)),
			V1Value::Duration(v) => {
				let seconds = v.0.as_secs();
				let nanos = v.0.subsec_nanos();

				let tag_value = match (seconds, nanos) {
					(0, 0) => Box::new(CborValue::Array(vec![])),
					(_, 0) => Box::new(CborValue::Array(vec![CborValue::Integer(seconds.into())])),
					_ => Box::new(CborValue::Array(vec![
						CborValue::Integer(seconds.into()),
						CborValue::Integer(nanos.into()),
					])),
				};

				Ok(CborValue::Tag(TAG_CUSTOM_DURATION, tag_value))
			}
			V1Value::Datetime(v) => {
				let seconds = v.0.timestamp();
				let nanos = v.0.timestamp_subsec_nanos();

				Ok(CborValue::Tag(
					TAG_CUSTOM_DATETIME,
					Box::new(CborValue::Array(vec![
						CborValue::Integer(seconds.into()),
						CborValue::Integer(nanos.into()),
					])),
				))
			}
			V1Value::Uuid(v) => Ok(CborValue::Tag(
				TAG_SPEC_UUID,
				Box::new(CborValue::Bytes(v.0.into_bytes().into())),
			)),
			V1Value::Array(v) => Ok(CborValue::Array(
				v.0.into_iter()
					.map(|v| {
						let v = v.into_cbor()?;
						Ok(v)
					})
					.collect::<Result<Vec<CborValue>, anyhow::Error>>()?,
			)),
			V1Value::Object(v) => Ok(CborValue::Map(
				v.into_iter()
					.map(|(k, v)| {
						let k = CborValue::Text(k);
						let v = v.into_cbor()?;
						Ok((k, v))
					})
					.collect::<Result<Vec<(CborValue, CborValue)>, anyhow::Error>>()?,
			)),
			V1Value::Bytes(v) => Ok(CborValue::Bytes(v.0)),
			V1Value::RecordId(v) => Ok(CborValue::Tag(
				TAG_RECORDID,
				Box::new(CborValue::Array(vec![
					CborValue::Text(v.tb),
					match v.id {
						V1Id::Number(v) => CborValue::Integer(v.into()),
						V1Id::String(v) => CborValue::Text(v),
						V1Id::Uuid(v) => V1Value::from(v).into_cbor()?,
						V1Id::Array(v) => V1Value::from(v).into_cbor()?,
						V1Id::Object(v) => V1Value::from(v).into_cbor()?,
						V1Id::Generate(_) => {
							return Err(anyhow::anyhow!(
								"Cannot encode an ungenerated Record ID into CBOR"
							));
						}
						V1Id::Range(v) => CborValue::Tag(TAG_RANGE, Box::new(v.into_cbor()?)),
					},
				])),
			)),
			V1Value::Table(v) => Ok(CborValue::Tag(TAG_TABLE, Box::new(CborValue::Text(v.0)))),
			V1Value::Geometry(v) => Ok(encode_geometry(v)?),
			V1Value::File(V1File {
				bucket,
				key,
			}) => Ok(CborValue::Tag(
				TAG_FILE,
				Box::new(CborValue::Array(vec![CborValue::Text(bucket), CborValue::Text(key)])),
			)),
			// We shouldn't reach here
			_ => anyhow::bail!("Found unsupported SurrealQL value being encoded into a CBOR value"),
		}
	}
}

fn encode_geometry(v: V1Geometry) -> Result<CborValue, anyhow::Error> {
	match v {
		V1Geometry::Point(v) => Ok(CborValue::Tag(
			TAG_GEOMETRY_POINT,
			Box::new(CborValue::Array(vec![CborValue::Float(v.x()), CborValue::Float(v.y())])),
		)),
		V1Geometry::Line(v) => {
			let data = v
				.points()
				.map(|v| encode_geometry(v.into()))
				.collect::<Result<Vec<CborValue>, anyhow::Error>>()?;

			Ok(CborValue::Tag(TAG_GEOMETRY_LINE, Box::new(CborValue::Array(data))))
		}
		V1Geometry::Polygon(v) => {
			let data = once(v.exterior())
				.chain(v.interiors())
				.map(|v| encode_geometry(v.clone().into()))
				.collect::<Result<Vec<CborValue>, anyhow::Error>>()?;

			Ok(CborValue::Tag(TAG_GEOMETRY_POLYGON, Box::new(CborValue::Array(data))))
		}
		V1Geometry::MultiPoint(v) => {
			let data = v
				.iter()
				.map(|v| encode_geometry((*v).into()))
				.collect::<Result<Vec<CborValue>, anyhow::Error>>()?;

			Ok(CborValue::Tag(TAG_GEOMETRY_MULTIPOINT, Box::new(CborValue::Array(data))))
		}
		V1Geometry::MultiLine(v) => {
			let data = v
				.iter()
				.map(|v| encode_geometry(v.clone().into()))
				.collect::<Result<Vec<CborValue>, anyhow::Error>>()?;

			Ok(CborValue::Tag(TAG_GEOMETRY_MULTILINE, Box::new(CborValue::Array(data))))
		}
		V1Geometry::MultiPolygon(v) => {
			let data = v
				.iter()
				.map(|v| encode_geometry(v.clone().into()))
				.collect::<Result<Vec<CborValue>, anyhow::Error>>()?;

			Ok(CborValue::Tag(TAG_GEOMETRY_MULTIPOLYGON, Box::new(CborValue::Array(data))))
		}
		V1Geometry::Collection(v) => {
			let data = v
				.iter()
				.map(|v| encode_geometry(v.clone()))
				.collect::<Result<Vec<CborValue>, anyhow::Error>>()?;

			Ok(CborValue::Tag(TAG_GEOMETRY_COLLECTION, Box::new(CborValue::Array(data))))
		}
	}
}

impl V1IdRange {
	pub fn from_cbor(val: CborValue) -> Result<Self, anyhow::Error> {
		fn decode_bound(v: CborValue) -> Result<Bound<V1Id>, anyhow::Error> {
			match v {
				CborValue::Tag(TAG_BOUND_INCLUDED, v) => Ok(Bound::Included(V1Id::from_cbor(*v)?)),
				CborValue::Tag(TAG_BOUND_EXCLUDED, v) => Ok(Bound::Excluded(V1Id::from_cbor(*v)?)),
				CborValue::Null => Ok(Bound::Unbounded),
				_ => Err(anyhow::anyhow!("Expected a bound tag")),
			}
		}

		match val {
			CborValue::Array(v) if v.len() == 2 => {
				let mut v = v;
				let beg = decode_bound(v.remove(0).clone())?;
				let end = decode_bound(v.remove(0).clone())?;
				Ok(V1IdRange::from((beg, end)))
			}
			_ => Err(anyhow::anyhow!("Expected a CBOR array with 2 bounds")),
		}
	}
}

impl V1IdRange {
	pub fn into_cbor(self) -> Result<CborValue, anyhow::Error> {
		fn encode(b: Bound<V1Id>) -> Result<CborValue, anyhow::Error> {
			match b {
				Bound::Included(v) => {
					Ok(CborValue::Tag(TAG_BOUND_INCLUDED, Box::new(v.into_cbor()?)))
				}
				Bound::Excluded(v) => {
					Ok(CborValue::Tag(TAG_BOUND_EXCLUDED, Box::new(v.into_cbor()?)))
				}
				Bound::Unbounded => Ok(CborValue::Null),
			}
		}

		Ok(CborValue::Array(vec![encode(self.beg)?, encode(self.end)?]))
	}
}

impl V1Id {
	pub fn from_cbor(val: CborValue) -> Result<Self, anyhow::Error> {
		match val {
			CborValue::Integer(v) => Ok(V1Id::Number(i128::from(v) as i64)),
			CborValue::Text(v) => Ok(V1Id::String(v)),
			CborValue::Array(v) => Ok(V1Id::Array(V1Array::from_cbor(v)?)),
			CborValue::Map(v) => Ok(V1Id::Object(V1Object::from_cbor(v)?)),
			CborValue::Tag(TAG_RANGE, v) => Ok(V1Id::Range(Box::new(V1IdRange::from_cbor(*v)?))),
			CborValue::Tag(TAG_STRING_UUID, v) => {
				V1Uuid::from_cbor(v.deref().to_owned()).map(V1Id::Uuid)
			}
			CborValue::Tag(TAG_SPEC_UUID, v) => {
				V1Uuid::from_cbor(v.deref().to_owned()).map(V1Id::Uuid)
			}
			_ => Err(anyhow::anyhow!("Expected a CBOR integer, text, array or map")),
		}
	}
}

impl V1Id {
	pub fn into_cbor(self) -> Result<CborValue, anyhow::Error> {
		match self {
			V1Id::Number(v) => Ok(CborValue::Integer(v.into())),
			V1Id::String(v) => Ok(CborValue::Text(v)),
			V1Id::Array(v) => Ok(V1Value::from(v).into_cbor()?),
			V1Id::Object(v) => Ok(V1Value::from(v).into_cbor()?),
			V1Id::Range(v) => {
				Ok(CborValue::Tag(TAG_RANGE, Box::new(v.deref().to_owned().into_cbor()?)))
			}
			V1Id::Uuid(v) => Ok(CborValue::Tag(
				TAG_SPEC_UUID,
				Box::new(CborValue::Bytes(v.0.into_bytes().into())),
			)),
			V1Id::Generate(_) => {
				Err(anyhow::anyhow!("Cannot encode an ungenerated Record ID into CBOR"))
			}
		}
	}
}

impl V1Array {
	pub fn from_cbor(val: Vec<CborValue>) -> Result<Self, anyhow::Error> {
		val.into_iter().map(|v| V1Value::from_cbor(v)).collect::<Result<V1Array, anyhow::Error>>()
	}
}

impl V1Object {
	pub fn from_cbor(val: Vec<(CborValue, CborValue)>) -> Result<Self, anyhow::Error> {
		Ok(V1Object(
			val.into_iter()
				.map(|(k, v)| {
					let k = V1Value::from_cbor(k).map(|k| k.as_string());
					let v = V1Value::from_cbor(v);
					Ok((k?, v?))
				})
				.collect::<Result<BTreeMap<String, V1Value>, anyhow::Error>>()?,
		))
	}
}

impl V1Uuid {
	pub fn from_cbor(val: CborValue) -> Result<Self, anyhow::Error> {
		match val {
			CborValue::Bytes(v) if v.len() == 16 => match v.as_slice().try_into() {
				Ok(v) => Ok(V1Uuid::from(uuid::Uuid::from_bytes(v))),
				Err(_) => Err(anyhow::anyhow!("Expected a CBOR byte array with 16 elements")),
			},
			_ => Err(anyhow::anyhow!("Expected a CBOR byte array with 16 elements")),
		}
	}
}
