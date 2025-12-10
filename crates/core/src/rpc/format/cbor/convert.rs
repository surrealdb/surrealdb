use std::collections::{BTreeMap, BTreeSet};
use std::iter::once;
use std::ops::Bound;

use anyhow::{Context, Result, anyhow};
use ciborium::Value as CborValue;
use geo::{LineString, Point, Polygon};
use geo_types::{MultiLineString, MultiPoint, MultiPolygon};
use rust_decimal::Decimal;

use crate::syn;
use crate::types::{
	PublicArray, PublicDatetime, PublicDuration, PublicFile, PublicGeometry, PublicNumber,
	PublicObject, PublicRange, PublicRecordId, PublicRecordIdKey, PublicRecordIdKeyRange,
	PublicSet, PublicTable, PublicUuid, PublicValue,
};
use crate::val::DecimalExt;

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
// unused but the for backwards compatibility kept around to maybe avoid using
// that tag again.
const _TAG_FUTURE: u64 = 15;

// Ranges (49->51 is unassigned)
const TAG_RANGE: u64 = 49;
const TAG_BOUND_INCLUDED: u64 = 50;
const TAG_BOUND_EXCLUDED: u64 = 51;

// Custom tags (55->60 is unassigned)
const TAG_FILE: u64 = 55;
const TAG_SET: u64 = 56;

// Custom Geometries (88->95 is unassigned)
const TAG_GEOMETRY_POINT: u64 = 88;
const TAG_GEOMETRY_LINE: u64 = 89;
const TAG_GEOMETRY_POLYGON: u64 = 90;
const TAG_GEOMETRY_MULTIPOINT: u64 = 91;
const TAG_GEOMETRY_MULTILINE: u64 = 92;
const TAG_GEOMETRY_MULTIPOLYGON: u64 = 93;
const TAG_GEOMETRY_COLLECTION: u64 = 94;

pub fn to_value(val: CborValue) -> Result<PublicValue> {
	match val {
		CborValue::Null => Ok(PublicValue::Null),
		CborValue::Bool(v) => Ok(PublicValue::Bool(v)),
		CborValue::Integer(v) => Ok(PublicValue::Number(PublicNumber::Int(i128::from(v) as i64))),
		CborValue::Float(v) => Ok(PublicValue::Number(PublicNumber::Float(v))),
		CborValue::Bytes(v) => Ok(PublicValue::Bytes(v.into())),
		CborValue::Text(v) => Ok(PublicValue::String(v)),
		CborValue::Array(v) => Ok(PublicValue::Array(to_array(v)?)),
		CborValue::Map(v) => Ok(PublicValue::Object(to_object(v)?)),
		CborValue::Tag(t, v) => {
			match t {
				// A literal datetime
				TAG_SPEC_DATETIME => match *v {
					CborValue::Text(v) => match syn::datetime(v.as_str()) {
						Ok(v) => Ok(PublicValue::Datetime(v)),
						_ => Err(anyhow!("Expected a valid Datetime value")),
					},
					_ => Err(anyhow!("Expected a CBOR text data type")),
				},
				// A custom [seconds: i64, nanos: u32] datetime
				TAG_CUSTOM_DATETIME => match *v {
					CborValue::Array(v) if v.len() == 2 => {
						let mut iter = v.into_iter();

						let seconds = match iter.next() {
							Some(CborValue::Integer(v)) => match i64::try_from(v) {
								Ok(v) => v,
								_ => return Err(anyhow!("Expected a CBOR integer data type")),
							},
							_ => return Err(anyhow!("Expected a CBOR integer data type")),
						};

						let nanos = match iter.next() {
							Some(CborValue::Integer(v)) => match u32::try_from(v) {
								Ok(v) => v,
								_ => return Err(anyhow!("Expected a CBOR integer data type")),
							},
							_ => return Err(anyhow!("Expected a CBOR integer data type")),
						};

						match PublicDatetime::try_from((seconds, nanos)) {
							Ok(v) => Ok(PublicValue::Datetime(v)),
							_ => Err(anyhow!("Expected a valid Datetime value")),
						}
					}
					_ => Err(anyhow!("Expected a CBOR array with 2 elements")),
				},
				// A literal NONE
				TAG_NONE => Ok(PublicValue::None),
				// A literal uuid
				TAG_STRING_UUID => match *v {
					CborValue::Text(v) => match v.parse::<PublicUuid>() {
						Ok(v) => Ok(PublicValue::Uuid(v)),
						_ => Err(anyhow!("Expected a valid UUID value")),
					},
					_ => Err(anyhow!("Expected a CBOR text data type")),
				},
				// A byte string uuid
				TAG_SPEC_UUID => to_uuid(*v).map(PublicValue::Uuid),
				// A literal decimal
				TAG_STRING_DECIMAL => match *v {
					CborValue::Text(v) => match Decimal::from_str_normalized(v.as_str()) {
						Ok(v) => Ok(PublicValue::Number(PublicNumber::Decimal(v))),
						_ => Err(anyhow!("Expected a valid Decimal value")),
					},
					_ => Err(anyhow!("Expected a CBOR text data type")),
				},
				// A literal duration
				TAG_STRING_DURATION => match *v {
					CborValue::Text(v) => match v.parse::<PublicDuration>() {
						Ok(v) => Ok(PublicValue::Duration(v)),
						_ => Err(anyhow!("Expected a valid Duration value")),
					},
					_ => Err(anyhow!("Expected a CBOR text data type")),
				},
				// A custom [seconds: Option<u64>, nanos: Option<u32>] duration
				TAG_CUSTOM_DURATION => match *v {
					CborValue::Array(v) if v.len() <= 2 => {
						let mut iter = v.into_iter();

						let seconds = match iter.next() {
							Some(CborValue::Integer(v)) => match u64::try_from(v) {
								Ok(v) => v,
								_ => return Err(anyhow!("Expected a CBOR integer data type")),
							},
							_ => 0,
						};

						let nanos = match iter.next() {
							Some(CborValue::Integer(v)) => match u32::try_from(v) {
								Ok(v) => v,
								_ => return Err(anyhow!("Expected a CBOR integer data type")),
							},
							_ => 0,
						};

						Ok(PublicValue::Duration(PublicDuration::new(seconds, nanos)))
					}
					_ => Err(anyhow!("Expected a CBOR array with at most 2 elements")),
				},
				// A literal recordid
				TAG_RECORDID => match *v {
					CborValue::Text(v) => match syn::record_id(v.as_str()) {
						Ok(v) => Ok(PublicValue::RecordId(v)),
						_ => Err(anyhow!("Expected a valid RecordID value")),
					},
					CborValue::Array(v) => {
						let mut v = v.into_iter();
						let table = v.next().context(
							"Expected a CBOR text data type, or a CBOR array with 2 elements: got empty array",
						)?;
						let key = v.next().context("Expected a CBOR text data type, or a CBOR array with 2 elements: got array with only one element")?;
						if v.next().is_some() {
							return Err(anyhow!(
								"Expected a CBOR text data type, or a CBOR array with 2 elements: got array with more than two elements"
							));
						}

						let table = to_value(table)?.into_string()?;

						let key = to_record_id_key(key)?;

						Ok(PublicValue::RecordId(PublicRecordId {
							table: table.into(),
							key,
						}))
					}
					_ => Err(anyhow!(
						"Expected a CBOR text data type, or a CBOR array with 2 elements"
					)),
				},
				// A literal table
				TAG_TABLE => match *v {
					CborValue::Text(v) => Ok(PublicValue::Table(PublicTable::new(v))),
					_ => Err(anyhow!("Expected a CBOR text data type")),
				},
				// A range
				TAG_RANGE => Ok(PublicValue::Range(Box::new(to_range(*v)?))),
				TAG_GEOMETRY_POINT => match *v {
					CborValue::Array(v) => {
						let err_msg = "Expected a CBOR array with 2 decimal values";
						let mut iter = v.into_iter();
						let x = iter.next().ok_or_else(|| anyhow!(err_msg))?;
						let y = iter.next().ok_or_else(|| anyhow!(err_msg))?;
						if iter.next().is_some() {
							return Err(anyhow!(err_msg));
						};

						let x = to_value(x)?;
						let y = to_value(y)?;

						match (x, y) {
							(PublicValue::Number(x), PublicValue::Number(y)) => {
								Ok(PublicValue::Geometry(PublicGeometry::Point(
									(
										x.to_f64().unwrap_or_default(),
										y.to_f64().unwrap_or_default(),
									)
										.into(),
								)))
							}
							_ => Err(anyhow!("Expected a CBOR array with 2 decimal values")),
						}
					}
					_ => Err(anyhow!("Expected a CBOR array with 2 decimal values")),
				},
				TAG_GEOMETRY_LINE => match *v {
					CborValue::Array(v) => {
						let points = v
							.into_iter()
							.map(|v| match to_value(v)? {
								PublicValue::Geometry(PublicGeometry::Point(v)) => Ok(v),
								_ => {
									Err(anyhow!("Expected a CBOR array with Geometry Point values"))
								}
							})
							.collect::<Result<Vec<Point>>>()?;

						Ok(PublicValue::Geometry(PublicGeometry::Line(LineString::from(points))))
					}
					_ => Err(anyhow!("Expected a CBOR array with Geometry Point values")),
				},
				TAG_GEOMETRY_POLYGON => match *v {
					CborValue::Array(v) if !v.is_empty() => {
						let lines = v
							.into_iter()
							.map(|v| match to_value(v)? {
								PublicValue::Geometry(PublicGeometry::Line(v)) => Ok(v),
								_ => {
									Err(anyhow!("Expected a CBOR array with Geometry Line values"))
								}
							})
							.collect::<Result<Vec<LineString>>>()?;

						let exterior = match lines.first() {
							Some(v) => v,
							_ => {
								return Err(anyhow!(
									"Expected a CBOR array with at least one Geometry Line values",
								));
							}
						};
						let interiors = Vec::from(&lines[1..]);

						Ok(PublicValue::Geometry(PublicGeometry::Polygon(Polygon::new(
							exterior.clone(),
							interiors,
						))))
					}
					_ => {
						Err(anyhow!("Expected a CBOR array with at least one Geometry Line values"))
					}
				},
				TAG_GEOMETRY_MULTIPOINT => match *v {
					CborValue::Array(v) => {
						let points = v
							.into_iter()
							.map(|v| match to_value(v)? {
								PublicValue::Geometry(PublicGeometry::Point(v)) => Ok(v),
								_ => {
									Err(anyhow!("Expected a CBOR array with Geometry Point values"))
								}
							})
							.collect::<Result<Vec<Point>>>()?;

						Ok(PublicValue::Geometry(PublicGeometry::MultiPoint(MultiPoint::from(
							points,
						))))
					}
					_ => Err(anyhow!("Expected a CBOR array with Geometry Point values")),
				},
				TAG_GEOMETRY_MULTILINE => match *v {
					CborValue::Array(v) => {
						let lines = v
							.into_iter()
							.map(|v| match to_value(v)? {
								PublicValue::Geometry(PublicGeometry::Line(v)) => Ok(v),
								_ => {
									Err(anyhow!("Expected a CBOR array with Geometry Line values"))
								}
							})
							.collect::<Result<Vec<LineString>>>()?;

						Ok(PublicValue::Geometry(PublicGeometry::MultiLine(MultiLineString::new(
							lines,
						))))
					}
					_ => Err(anyhow!("Expected a CBOR array with Geometry Line values")),
				},
				TAG_GEOMETRY_MULTIPOLYGON => match *v {
					CborValue::Array(v) => {
						let polygons = v
							.into_iter()
							.map(|v| match to_value(v)? {
								PublicValue::Geometry(PublicGeometry::Polygon(v)) => Ok(v),
								_ => Err(anyhow!(
									"Expected a CBOR array with Geometry Polygon values"
								)),
							})
							.collect::<Result<Vec<Polygon>>>()?;

						Ok(PublicValue::Geometry(PublicGeometry::MultiPolygon(MultiPolygon::from(
							polygons,
						))))
					}
					_ => Err(anyhow!("Expected a CBOR array with Geometry Polygon values")),
				},
				TAG_GEOMETRY_COLLECTION => match *v {
					CborValue::Array(v) => {
						let geometries = v
							.into_iter()
							.map(|v| match to_value(v)? {
								PublicValue::Geometry(v) => Ok(v),
								_ => Err(anyhow!("Expected a CBOR array with Geometry values")),
							})
							.collect::<Result<Vec<PublicGeometry>>>()?;

						Ok(PublicValue::Geometry(PublicGeometry::Collection(geometries)))
					}
					_ => Err(anyhow!("Expected a CBOR array with Geometry values")),
				},
				TAG_FILE => match *v {
					CborValue::Array(mut v) if v.len() == 2 => {
						let CborValue::Text(bucket) = v.remove(0) else {
							return Err(anyhow!("Expected the bucket name to be a string value"));
						};

						let CborValue::Text(key) = v.remove(0) else {
							return Err(anyhow!("Expected the file key to be a string value"));
						};

						Ok(PublicValue::File(PublicFile::new(bucket, key)))
					}
					_ => {
						Err(anyhow!("Expected a CBOR array with two String bucket and key values"))
					}
				},
				TAG_SET => match *v {
					CborValue::Array(v) => Ok(PublicValue::Set(PublicSet::from(
						v.into_iter().map(to_value).collect::<Result<BTreeSet<PublicValue>>>()?,
					))),
					_ => Err(anyhow!("Expected a CBOR array with Set values")),
				},
				// An unknown tag
				_ => Err(anyhow!("Encountered an unknown CBOR tag")),
			}
		}
		_ => Err(anyhow!("Encountered an unknown CBOR data type")),
	}
}

pub fn from_value(val: PublicValue) -> Result<CborValue> {
	match val {
		PublicValue::None => Ok(CborValue::Tag(TAG_NONE, Box::new(CborValue::Null))),
		PublicValue::Null => Ok(CborValue::Null),
		PublicValue::Bool(v) => Ok(CborValue::Bool(v)),
		PublicValue::Number(v) => match v {
			PublicNumber::Int(v) => Ok(CborValue::Integer(v.into())),
			PublicNumber::Float(v) => Ok(CborValue::Float(v)),
			PublicNumber::Decimal(v) => {
				Ok(CborValue::Tag(TAG_STRING_DECIMAL, Box::new(CborValue::Text(v.to_string()))))
			}
		},
		PublicValue::String(v) => Ok(CborValue::Text(v)),
		PublicValue::Duration(v) => {
			let seconds = v.secs();
			let nanos = v.subsec_nanos();

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
		PublicValue::Datetime(v) => {
			let seconds = v.timestamp();
			let nanos = v.timestamp_subsec_nanos();

			Ok(CborValue::Tag(
				TAG_CUSTOM_DATETIME,
				Box::new(CborValue::Array(vec![
					CborValue::Integer(seconds.into()),
					CborValue::Integer(nanos.into()),
				])),
			))
		}
		PublicValue::Uuid(v) => {
			Ok(CborValue::Tag(TAG_SPEC_UUID, Box::new(CborValue::Bytes(v.into_bytes().into()))))
		}
		PublicValue::Array(v) => from_array(v),
		PublicValue::Object(v) => from_object(v),
		PublicValue::Bytes(v) => Ok(CborValue::Bytes(v.into_inner().to_vec())),
		PublicValue::Table(v) => {
			Ok(CborValue::Tag(TAG_TABLE, Box::new(CborValue::Text(v.into_string()))))
		}
		PublicValue::RecordId(PublicRecordId {
			table,
			key,
		}) => Ok(CborValue::Tag(
			TAG_RECORDID,
			Box::new(CborValue::Array(vec![
				CborValue::Text(table.into_string()),
				match key {
					PublicRecordIdKey::Number(v) => CborValue::Integer(v.into()),
					PublicRecordIdKey::String(v) => CborValue::Text(v),
					PublicRecordIdKey::Uuid(v) => from_uuid(v),
					PublicRecordIdKey::Array(v) => from_array(v)?,
					PublicRecordIdKey::Object(v) => from_object(v)?,
					PublicRecordIdKey::Range(v) => {
						CborValue::Tag(TAG_RANGE, Box::new(from_record_id_key_range(*v)?))
					}
				},
			])),
		)),
		PublicValue::Geometry(v) => from_geometry(v),
		PublicValue::Range(v) => Ok(CborValue::Tag(TAG_RANGE, Box::new(from_range(*v)?))),
		PublicValue::File(file) => Ok(CborValue::Tag(
			TAG_FILE,
			Box::new(CborValue::Array(vec![
				CborValue::Text(file.bucket),
				CborValue::Text(file.key),
			])),
		)),
		PublicValue::Set(v) => Ok(CborValue::Tag(
			TAG_SET,
			Box::new(CborValue::Array(
				v.into_iter().map(from_value).collect::<Result<Vec<CborValue>>>()?,
			)),
		)),
		PublicValue::Regex(_) => {
			// Uncborrable value type
			Err(anyhow!("Unsupported value type: Regex"))
		}
	}
}

fn from_geometry(v: PublicGeometry) -> Result<CborValue> {
	match v {
		PublicGeometry::Point(v) => Ok(CborValue::Tag(
			TAG_GEOMETRY_POINT,
			Box::new(CborValue::Array(vec![CborValue::Float(v.x()), CborValue::Float(v.y())])),
		)),
		PublicGeometry::Line(v) => {
			let data = v
				.points()
				.map(|v| from_geometry(PublicGeometry::Point(v)))
				.collect::<Result<Vec<CborValue>>>()?;

			Ok(CborValue::Tag(TAG_GEOMETRY_LINE, Box::new(CborValue::Array(data))))
		}
		PublicGeometry::Polygon(v) => {
			let data = once(v.exterior())
				.chain(v.interiors())
				.map(|v| from_geometry(PublicGeometry::Line(v.clone())))
				.collect::<Result<Vec<CborValue>>>()?;

			Ok(CborValue::Tag(TAG_GEOMETRY_POLYGON, Box::new(CborValue::Array(data))))
		}
		PublicGeometry::MultiPoint(v) => {
			let data = v
				.into_iter()
				.map(|v| from_geometry(PublicGeometry::Point(v)))
				.collect::<Result<Vec<CborValue>>>()?;

			Ok(CborValue::Tag(TAG_GEOMETRY_MULTIPOINT, Box::new(CborValue::Array(data))))
		}
		PublicGeometry::MultiLine(v) => {
			let data = v
				.into_iter()
				.map(|v| from_geometry(PublicGeometry::Line(v)))
				.collect::<Result<Vec<CborValue>>>()?;

			Ok(CborValue::Tag(TAG_GEOMETRY_MULTILINE, Box::new(CborValue::Array(data))))
		}
		PublicGeometry::MultiPolygon(v) => {
			let data = v
				.into_iter()
				.map(|v| from_geometry(PublicGeometry::Polygon(v)))
				.collect::<Result<Vec<CborValue>>>()?;

			Ok(CborValue::Tag(TAG_GEOMETRY_MULTIPOLYGON, Box::new(CborValue::Array(data))))
		}
		PublicGeometry::Collection(v) => {
			let data = v.into_iter().map(from_geometry).collect::<Result<Vec<CborValue>>>()?;

			Ok(CborValue::Tag(TAG_GEOMETRY_COLLECTION, Box::new(CborValue::Array(data))))
		}
	}
}

fn to_range(val: CborValue) -> Result<PublicRange> {
	fn decode_bound(v: CborValue) -> Result<Bound<PublicValue>> {
		match v {
			CborValue::Tag(TAG_BOUND_INCLUDED, v) => Ok(Bound::Included(to_value(*v)?)),
			CborValue::Tag(TAG_BOUND_EXCLUDED, v) => Ok(Bound::Excluded(to_value(*v)?)),
			CborValue::Null => Ok(Bound::Unbounded),
			_ => Err(anyhow!("Expected a bound tag")),
		}
	}

	match val {
		CborValue::Array(v) if v.len() == 2 => {
			let mut v = v;
			let beg = decode_bound(v.remove(0))?;
			let end = decode_bound(v.remove(0))?;
			Ok(PublicRange::new(beg, end))
		}
		_ => Err(anyhow!("Expected a CBOR array with 2 bounds")),
	}
}

fn from_range(r: PublicRange) -> Result<CborValue> {
	fn encode(b: Bound<PublicValue>) -> Result<CborValue> {
		match b {
			Bound::Included(v) => Ok(CborValue::Tag(TAG_BOUND_INCLUDED, Box::new(from_value(v)?))),
			Bound::Excluded(v) => Ok(CborValue::Tag(TAG_BOUND_EXCLUDED, Box::new(from_value(v)?))),
			Bound::Unbounded => Ok(CborValue::Null),
		}
	}
	let (start, end) = r.into_inner();
	Ok(CborValue::Array(vec![encode(start)?, encode(end)?]))
}

fn from_record_id_key_range(r: PublicRecordIdKeyRange) -> Result<CborValue> {
	fn encode(b: Bound<PublicRecordIdKey>) -> Result<CborValue> {
		match b {
			Bound::Included(v) => {
				Ok(CborValue::Tag(TAG_BOUND_INCLUDED, Box::new(from_record_id_key(v)?)))
			}
			Bound::Excluded(v) => {
				Ok(CborValue::Tag(TAG_BOUND_EXCLUDED, Box::new(from_record_id_key(v)?)))
			}
			Bound::Unbounded => Ok(CborValue::Null),
		}
	}

	let (start, end) = r.into_inner();
	Ok(CborValue::Array(vec![encode(start)?, encode(end)?]))
}

fn to_record_id_key_range(val: CborValue) -> Result<PublicRecordIdKeyRange> {
	fn decode_bound(v: CborValue) -> Result<Bound<PublicRecordIdKey>> {
		match v {
			CborValue::Tag(TAG_BOUND_INCLUDED, v) => Ok(Bound::Included(to_record_id_key(*v)?)),
			CborValue::Tag(TAG_BOUND_EXCLUDED, v) => Ok(Bound::Excluded(to_record_id_key(*v)?)),
			CborValue::Null => Ok(Bound::Unbounded),
			_ => Err(anyhow!("Expected a bound tag")),
		}
	}

	match val {
		CborValue::Array(v) if v.len() == 2 => {
			let mut v = v;
			let start = decode_bound(v.remove(0))?;
			let end = decode_bound(v.remove(0))?;

			Ok(PublicRecordIdKeyRange {
				start,
				end,
			})
		}
		_ => Err(anyhow!("Expected a CBOR array with 2 bounds")),
	}
}

fn from_record_id_key(v: PublicRecordIdKey) -> Result<CborValue> {
	match v {
		PublicRecordIdKey::Number(v) => Ok(CborValue::Integer(v.into())),
		PublicRecordIdKey::String(v) => Ok(CborValue::Text(v)),
		PublicRecordIdKey::Array(v) => from_array(v),
		PublicRecordIdKey::Object(v) => from_object(v),
		PublicRecordIdKey::Range(v) => {
			Ok(CborValue::Tag(TAG_RANGE, Box::new(from_record_id_key_range(*v)?)))
		}
		PublicRecordIdKey::Uuid(v) => {
			Ok(CborValue::Tag(TAG_SPEC_UUID, Box::new(CborValue::Bytes(v.into_bytes().into()))))
		}
	}
}

fn to_record_id_key(val: CborValue) -> Result<PublicRecordIdKey> {
	match val {
		CborValue::Integer(v) => Ok(PublicRecordIdKey::Number(i128::from(v) as i64)),
		CborValue::Text(v) => Ok(PublicRecordIdKey::String(v)),
		CborValue::Array(v) => Ok(PublicRecordIdKey::Array(to_array(v)?)),
		CborValue::Map(v) => Ok(PublicRecordIdKey::Object(to_object(v)?)),
		CborValue::Tag(TAG_RANGE, v) => {
			Ok(PublicRecordIdKey::Range(Box::new(to_record_id_key_range(*v)?)))
		}
		CborValue::Tag(TAG_STRING_UUID, v) => match *v {
			CborValue::Text(v) => match PublicUuid::try_from(v) {
				Ok(v) => Ok(PublicRecordIdKey::Uuid(v)),
				_ => Err(anyhow!("Expected a valid UUID value")),
			},
			_ => Err(anyhow!("Expected a CBOR text data type")),
		},
		CborValue::Tag(TAG_SPEC_UUID, v) => to_uuid(*v).map(PublicRecordIdKey::Uuid),
		_ => Err(anyhow!("Expected a CBOR integer, text, array or map")),
	}
}

fn from_uuid(val: PublicUuid) -> CborValue {
	CborValue::Tag(TAG_SPEC_UUID, Box::new(CborValue::Bytes(val.into_bytes().into())))
}

fn to_uuid(val: CborValue) -> Result<PublicUuid> {
	match val {
		CborValue::Bytes(v) if v.len() == 16 => match v.as_slice().try_into() {
			Ok(v) => Ok(PublicUuid::from(uuid::Uuid::from_bytes(v))),
			Err(_) => Err(anyhow!("Expected a CBOR byte array with 16 elements")),
		},
		_ => Err(anyhow!("Expected a CBOR byte array with 16 elements")),
	}
}

fn from_array(array: PublicArray) -> Result<CborValue> {
	array
		.into_iter()
		.map(|v| {
			let v = from_value(v)?;
			Ok(v)
		})
		.collect::<Result<Vec<CborValue>>>()
		.map(CborValue::Array)
}

fn to_array(array: Vec<CborValue>) -> Result<PublicArray> {
	Ok(array.into_iter().map(to_value).collect::<Result<Vec<PublicValue>, _>>()?.into())
}

fn from_object(obj: PublicObject) -> Result<CborValue> {
	obj.into_iter()
		.map(|(k, v)| {
			let k = CborValue::Text(k);
			let v = from_value(v)?;
			Ok((k, v))
		})
		.collect::<Result<Vec<(CborValue, CborValue)>>>()
		.map(CborValue::Map)
}

fn to_object(obj: Vec<(CborValue, CborValue)>) -> Result<PublicObject> {
	let res = obj
		.into_iter()
		.map(|(k, v)| {
			let CborValue::Text(k) = k else {
				return Err(anyhow!("Expected object key to be a string"));
			};
			let v = to_value(v)?;
			Ok((k, v))
		})
		.collect::<Result<BTreeMap<_, _>>>()?;
	Ok(PublicObject::from(res))
}
