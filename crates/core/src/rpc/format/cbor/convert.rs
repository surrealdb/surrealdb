use std::collections::BTreeMap;
use std::iter::once;
use std::ops::Bound;

use ciborium::Value as CborValue;
use geo::{LineString, Point, Polygon};
use geo_types::{MultiLineString, MultiPoint, MultiPolygon};
use rust_decimal::Decimal;

use crate::syn;
use crate::val::{
	self, Array, DecimalExt, Geometry, Number, Object, Range, RecordIdKey, RecordIdKeyRange, Table,
	Uuid, Value,
};

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

// Custom Geometries (88->95 is unassigned)
const TAG_GEOMETRY_POINT: u64 = 88;
const TAG_GEOMETRY_LINE: u64 = 89;
const TAG_GEOMETRY_POLYGON: u64 = 90;
const TAG_GEOMETRY_MULTIPOINT: u64 = 91;
const TAG_GEOMETRY_MULTILINE: u64 = 92;
const TAG_GEOMETRY_MULTIPOLYGON: u64 = 93;
const TAG_GEOMETRY_COLLECTION: u64 = 94;

pub fn to_value(val: CborValue) -> Result<Value, &'static str> {
	match val {
		CborValue::Null => Ok(val::Value::Null),
		CborValue::Bool(v) => Ok(val::Value::from(v)),
		CborValue::Integer(v) => Ok(val::Value::from(i128::from(v))),
		CborValue::Float(v) => Ok(val::Value::from(v)),
		CborValue::Bytes(v) => Ok(val::Value::Bytes(v.into())),
		CborValue::Text(v) => Ok(val::Value::from(v)),
		CborValue::Array(v) => Ok(val::Value::Array(to_array(v)?)),
		CborValue::Map(v) => Ok(val::Value::Object(to_object(v)?)),
		CborValue::Tag(t, v) => {
			match t {
				// A literal datetime
				TAG_SPEC_DATETIME => match *v {
					CborValue::Text(v) => match syn::datetime(v.as_str()) {
						Ok(v) => Ok(v.into()),
						_ => Err("Expected a valid val::Datetime value"),
					},
					_ => Err("Expected a CBOR text data type"),
				},
				// A custom [seconds: i64, nanos: u32] datetime
				TAG_CUSTOM_DATETIME => match *v {
					CborValue::Array(v) if v.len() == 2 => {
						let mut iter = v.into_iter();

						let seconds = match iter.next() {
							Some(CborValue::Integer(v)) => match i64::try_from(v) {
								Ok(v) => v,
								_ => return Err("Expected a CBOR integer data type"),
							},
							_ => return Err("Expected a CBOR integer data type"),
						};

						let nanos = match iter.next() {
							Some(CborValue::Integer(v)) => match u32::try_from(v) {
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
					CborValue::Text(v) => match val::Uuid::try_from(v) {
						Ok(v) => Ok(v.into()),
						_ => Err("Expected a valid UUID value"),
					},
					_ => Err("Expected a CBOR text data type"),
				},
				// A byte string uuid
				TAG_SPEC_UUID => to_uuid(*v).map(Value::from),
				// A literal decimal
				TAG_STRING_DECIMAL => match *v {
					CborValue::Text(v) => match Decimal::from_str_normalized(v.as_str()) {
						Ok(v) => Ok(v.into()),
						_ => Err("Expected a valid Decimal value"),
					},
					_ => Err("Expected a CBOR text data type"),
				},
				// A literal duration
				TAG_STRING_DURATION => match *v {
					CborValue::Text(v) => match val::Duration::try_from(v) {
						Ok(v) => Ok(v.into()),
						_ => Err("Expected a valid val::Duration value"),
					},
					_ => Err("Expected a CBOR text data type"),
				},
				// A custom [seconds: Option<u64>, nanos: Option<u32>] duration
				TAG_CUSTOM_DURATION => match *v {
					CborValue::Array(v) if v.len() <= 2 => {
						let mut iter = v.into_iter();

						let seconds = match iter.next() {
							Some(CborValue::Integer(v)) => match u64::try_from(v) {
								Ok(v) => v,
								_ => return Err("Expected a CBOR integer data type"),
							},
							_ => 0,
						};

						let nanos = match iter.next() {
							Some(CborValue::Integer(v)) => match u32::try_from(v) {
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
					CborValue::Text(v) => match syn::record_id(v.as_str()) {
						Ok(v) => Ok(v.into()),
						_ => Err("Expected a valid RecordID value"),
					},
					CborValue::Array(v) => {
						let err = "Expected a CBOR text data type, or a CBOR array with 2 elements";
						let mut v = v.into_iter();
						let table = v.next().ok_or(err)?;
						let key = v.next().ok_or(err)?;
						if v.next().is_some() {
							return Err(err);
						}

						let table = match to_value(table) {
							Ok(val::Value::Strand(tb)) => tb.into_string(),
							Ok(val::Value::Table(tb)) => tb.into_string(),
							_ => {
								return Err(
									"Expected the tb of a Record Id to be a String or Table value",
								);
							}
						};

						let key = to_record_id_key(key)?;

						Ok(val::Value::RecordId(val::RecordId {
							table,
							key,
						}))
					}
					_ => Err("Expected a CBOR text data type, or a CBOR array with 2 elements"),
				},
				// A literal table
				TAG_TABLE => match *v {
					// TODO: Null byte validitY
					CborValue::Text(v) => Ok(val::Value::Table(Table::new(v).unwrap())),
					_ => Err("Expected a CBOR text data type"),
				},
				// A range
				TAG_RANGE => Ok(val::Value::Range(Box::new(to_range(*v)?))),
				TAG_GEOMETRY_POINT => match *v {
					CborValue::Array(v) => {
						let err = "Expected a CBOR array with 2 decimal values";
						let mut iter = v.into_iter();
						let x = iter.next().ok_or(err)?;
						let y = iter.next().ok_or(err)?;
						if iter.next().is_some() {
							return Err(err);
						};

						let x = to_value(x)?;
						let y = to_value(y)?;

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
				TAG_GEOMETRY_LINE => match *v {
					CborValue::Array(v) => {
						let points = v
							.into_iter()
							.map(|v| match to_value(v)? {
								val::Value::Geometry(val::Geometry::Point(v)) => Ok(v),
								_ => Err("Expected a CBOR array with Geometry Point values"),
							})
							.collect::<Result<Vec<Point>, &str>>()?;

						Ok(val::Value::Geometry(val::Geometry::Line(LineString::from(points))))
					}
					_ => Err("Expected a CBOR array with Geometry Point values"),
				},
				TAG_GEOMETRY_POLYGON => match *v {
					CborValue::Array(v) if !v.is_empty() => {
						let lines = v
							.into_iter()
							.map(|v| match to_value(v)? {
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
				TAG_GEOMETRY_MULTIPOINT => match *v {
					CborValue::Array(v) => {
						let points = v
							.into_iter()
							.map(|v| match to_value(v)? {
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
				TAG_GEOMETRY_MULTILINE => match *v {
					CborValue::Array(v) => {
						let lines = v
							.into_iter()
							.map(|v| match to_value(v)? {
								val::Value::Geometry(val::Geometry::Line(v)) => Ok(v),
								_ => Err("Expected a CBOR array with Geometry Line values"),
							})
							.collect::<Result<Vec<LineString>, &str>>()?;

						Ok(val::Value::Geometry(val::Geometry::MultiLine(MultiLineString::new(
							lines,
						))))
					}
					_ => Err("Expected a CBOR array with Geometry Line values"),
				},
				TAG_GEOMETRY_MULTIPOLYGON => match *v {
					CborValue::Array(v) => {
						let polygons = v
							.into_iter()
							.map(|v| match to_value(v)? {
								val::Value::Geometry(val::Geometry::Polygon(v)) => Ok(v),
								_ => Err("Expected a CBOR array with Geometry Polygon values"),
							})
							.collect::<Result<Vec<Polygon>, &str>>()?;

						Ok(val::Value::Geometry(val::Geometry::MultiPolygon(MultiPolygon::from(
							polygons,
						))))
					}
					_ => Err("Expected a CBOR array with Geometry Polygon values"),
				},
				TAG_GEOMETRY_COLLECTION => match *v {
					CborValue::Array(v) => {
						let geometries = v
							.into_iter()
							.map(|v| match to_value(v)? {
								val::Value::Geometry(v) => Ok(v),
								_ => Err("Expected a CBOR array with Geometry values"),
							})
							.collect::<Result<Vec<val::Geometry>, &str>>()?;

						Ok(val::Value::Geometry(val::Geometry::Collection(geometries)))
					}
					_ => Err("Expected a CBOR array with Geometry values"),
				},
				TAG_FILE => match *v {
					CborValue::Array(mut v) if v.len() == 2 => {
						let CborValue::Text(bucket) = v.remove(0) else {
							return Err("Expected the bucket name to be a string value");
						};

						let CborValue::Text(key) = v.remove(0) else {
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

pub fn from_value(val: Value) -> Result<CborValue, &'static str> {
	match val {
		Value::None => Ok(CborValue::Tag(TAG_NONE, Box::new(CborValue::Null))),
		Value::Null => Ok(CborValue::Null),
		Value::Bool(v) => Ok(CborValue::Bool(v)),
		Value::Number(v) => match v {
			Number::Int(v) => Ok(CborValue::Integer(v.into())),
			Number::Float(v) => Ok(CborValue::Float(v)),
			Number::Decimal(v) => {
				Ok(CborValue::Tag(TAG_STRING_DECIMAL, Box::new(CborValue::Text(v.to_string()))))
			}
		},
		Value::Strand(v) => Ok(CborValue::Text(v.into_string())),
		Value::Duration(v) => {
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
		Value::Datetime(v) => {
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
		Value::Uuid(v) => {
			Ok(CborValue::Tag(TAG_SPEC_UUID, Box::new(CborValue::Bytes(v.into_bytes().into()))))
		}
		Value::Array(v) => from_array(v),
		Value::Object(v) => from_object(v),
		Value::Bytes(v) => Ok(CborValue::Bytes(v.into_inner())),
		Value::RecordId(v) => Ok(CborValue::Tag(
			TAG_RECORDID,
			Box::new(CborValue::Array(vec![
				CborValue::Text(v.table),
				match v.key {
					RecordIdKey::Number(v) => CborValue::Integer(v.into()),
					RecordIdKey::String(v) => CborValue::Text(v),
					RecordIdKey::Uuid(v) => from_uuid(v),
					RecordIdKey::Array(v) => from_array(v)?,
					RecordIdKey::Object(v) => from_object(v)?,
					RecordIdKey::Range(v) => {
						CborValue::Tag(TAG_RANGE, Box::new(from_record_id_key_range(*v)?))
					}
				},
			])),
		)),
		Value::Table(v) => {
			Ok(CborValue::Tag(TAG_TABLE, Box::new(CborValue::Text(v.into_string()))))
		}
		Value::Geometry(v) => from_geometry(v),
		Value::Range(v) => Ok(CborValue::Tag(TAG_RANGE, Box::new(from_range(*v)?))),
		Value::File(val::File {
			bucket,
			key,
		}) => Ok(CborValue::Tag(
			TAG_FILE,
			Box::new(CborValue::Array(vec![CborValue::Text(bucket), CborValue::Text(key)])),
		)),
		// We shouldn't reach here
		_ => Err("Found unsupported SurrealQL value being encoded into a CBOR value"),
	}
}

fn from_geometry(v: Geometry) -> Result<CborValue, &'static str> {
	match v {
		Geometry::Point(v) => Ok(CborValue::Tag(
			TAG_GEOMETRY_POINT,
			Box::new(CborValue::Array(vec![CborValue::Float(v.x()), CborValue::Float(v.y())])),
		)),
		Geometry::Line(v) => {
			let data = v
				.points()
				.map(|v| from_geometry(v.into()))
				.collect::<Result<Vec<CborValue>, &'static str>>()?;

			Ok(CborValue::Tag(TAG_GEOMETRY_LINE, Box::new(CborValue::Array(data))))
		}
		Geometry::Polygon(v) => {
			let data = once(v.exterior())
				.chain(v.interiors())
				.map(|v| from_geometry(v.clone().into()))
				.collect::<Result<Vec<CborValue>, &'static str>>()?;

			Ok(CborValue::Tag(TAG_GEOMETRY_POLYGON, Box::new(CborValue::Array(data))))
		}
		Geometry::MultiPoint(v) => {
			let data = v
				.into_iter()
				.map(|v| from_geometry(v.into()))
				.collect::<Result<Vec<CborValue>, &'static str>>()?;

			Ok(CborValue::Tag(TAG_GEOMETRY_MULTIPOINT, Box::new(CborValue::Array(data))))
		}
		Geometry::MultiLine(v) => {
			let data = v
				.into_iter()
				.map(|v| from_geometry(v.into()))
				.collect::<Result<Vec<CborValue>, &'static str>>()?;

			Ok(CborValue::Tag(TAG_GEOMETRY_MULTILINE, Box::new(CborValue::Array(data))))
		}
		Geometry::MultiPolygon(v) => {
			let data = v
				.into_iter()
				.map(|v| from_geometry(v.into()))
				.collect::<Result<Vec<CborValue>, &'static str>>()?;

			Ok(CborValue::Tag(TAG_GEOMETRY_MULTIPOLYGON, Box::new(CborValue::Array(data))))
		}
		Geometry::Collection(v) => {
			let data = v
				.into_iter()
				.map(|v| from_geometry(v))
				.collect::<Result<Vec<CborValue>, &'static str>>()?;

			Ok(CborValue::Tag(TAG_GEOMETRY_COLLECTION, Box::new(CborValue::Array(data))))
		}
	}
}

fn to_range(val: CborValue) -> Result<Range, &'static str> {
	fn decode_bound(v: CborValue) -> Result<Bound<Value>, &'static str> {
		match v {
			CborValue::Tag(TAG_BOUND_INCLUDED, v) => Ok(Bound::Included(to_value(*v)?)),
			CborValue::Tag(TAG_BOUND_EXCLUDED, v) => Ok(Bound::Excluded(to_value(*v)?)),
			CborValue::Null => Ok(Bound::Unbounded),
			_ => Err("Expected a bound tag"),
		}
	}

	match val {
		CborValue::Array(v) if v.len() == 2 => {
			let mut v = v;
			let beg = decode_bound(v.remove(0).clone())?;
			let end = decode_bound(v.remove(0).clone())?;
			Ok(Range::new(beg, end))
		}
		_ => Err("Expected a CBOR array with 2 bounds"),
	}
}

fn from_range(r: Range) -> Result<CborValue, &'static str> {
	fn encode(b: Bound<Value>) -> Result<CborValue, &'static str> {
		match b {
			Bound::Included(v) => Ok(CborValue::Tag(TAG_BOUND_INCLUDED, Box::new(from_value(v)?))),
			Bound::Excluded(v) => Ok(CborValue::Tag(TAG_BOUND_EXCLUDED, Box::new(from_value(v)?))),
			Bound::Unbounded => Ok(CborValue::Null),
		}
	}

	Ok(CborValue::Array(vec![encode(r.start)?, encode(r.end)?]))
}

fn from_record_id_key_range(r: RecordIdKeyRange) -> Result<CborValue, &'static str> {
	fn encode(b: Bound<RecordIdKey>) -> Result<CborValue, &'static str> {
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

	Ok(CborValue::Array(vec![encode(r.start)?, encode(r.end)?]))
}

fn to_record_id_key_range(val: CborValue) -> Result<RecordIdKeyRange, &'static str> {
	fn decode_bound(v: CborValue) -> Result<Bound<RecordIdKey>, &'static str> {
		match v {
			CborValue::Tag(TAG_BOUND_INCLUDED, v) => Ok(Bound::Included(to_record_id_key(*v)?)),
			CborValue::Tag(TAG_BOUND_EXCLUDED, v) => Ok(Bound::Excluded(to_record_id_key(*v)?)),
			CborValue::Null => Ok(Bound::Unbounded),
			_ => Err("Expected a bound tag"),
		}
	}

	match val {
		CborValue::Array(v) if v.len() == 2 => {
			let mut v = v;
			let start = decode_bound(v.remove(0).clone())?;
			let end = decode_bound(v.remove(0).clone())?;

			Ok(RecordIdKeyRange {
				start,
				end,
			})
		}
		_ => Err("Expected a CBOR array with 2 bounds"),
	}
}

fn from_record_id_key(v: RecordIdKey) -> Result<CborValue, &'static str> {
	match v {
		RecordIdKey::Number(v) => Ok(CborValue::Integer(v.into())),
		RecordIdKey::String(v) => Ok(CborValue::Text(v)),
		RecordIdKey::Array(v) => from_array(v),
		RecordIdKey::Object(v) => from_object(v),
		RecordIdKey::Range(v) => {
			Ok(CborValue::Tag(TAG_RANGE, Box::new(from_record_id_key_range(*v)?)))
		}
		RecordIdKey::Uuid(v) => {
			Ok(CborValue::Tag(TAG_SPEC_UUID, Box::new(CborValue::Bytes(v.into_bytes().into()))))
		}
	}
}

fn to_record_id_key(val: CborValue) -> Result<RecordIdKey, &'static str> {
	match val {
		CborValue::Integer(v) => Ok(RecordIdKey::Number(i128::from(v) as i64)),
		CborValue::Text(v) => Ok(RecordIdKey::String(v)),
		CborValue::Array(v) => Ok(RecordIdKey::Array(to_array(v)?)),
		CborValue::Map(v) => Ok(RecordIdKey::Object(to_object(v)?)),
		CborValue::Tag(TAG_RANGE, v) => {
			Ok(RecordIdKey::Range(Box::new(to_record_id_key_range(*v)?)))
		}
		CborValue::Tag(TAG_STRING_UUID, v) => match *v {
			CborValue::Text(v) => match val::Uuid::try_from(v) {
				Ok(v) => Ok(RecordIdKey::Uuid(v)),
				_ => Err("Expected a valid UUID value"),
			},
			_ => Err("Expected a CBOR text data type"),
		},
		CborValue::Tag(TAG_SPEC_UUID, v) => to_uuid(*v).map(RecordIdKey::Uuid),
		_ => Err("Expected a CBOR integer, text, array or map"),
	}
}

fn from_uuid(val: Uuid) -> CborValue {
	CborValue::Tag(TAG_SPEC_UUID, Box::new(CborValue::Bytes(val.into_bytes().into())))
}

fn to_uuid(val: CborValue) -> Result<Uuid, &'static str> {
	match val {
		CborValue::Bytes(v) if v.len() == 16 => match v.as_slice().try_into() {
			Ok(v) => Ok(Uuid::from(uuid::Uuid::from_bytes(v))),
			Err(_) => Err("Expected a CBOR byte array with 16 elements"),
		},
		_ => Err("Expected a CBOR byte array with 16 elements"),
	}
}

fn from_array(array: Array) -> Result<CborValue, &'static str> {
	array
		.into_iter()
		.map(|v| {
			let v = from_value(v)?;
			Ok(v)
		})
		.collect::<Result<Vec<CborValue>, &str>>()
		.map(CborValue::Array)
}

fn to_array(array: Vec<CborValue>) -> Result<Array, &'static str> {
	Ok(array.into_iter().map(to_value).collect::<Result<Vec<Value>, _>>()?.into())
}

fn from_object(obj: Object) -> Result<CborValue, &'static str> {
	obj.into_iter()
		.map(|(k, v)| {
			let k = CborValue::Text(k);
			let v = from_value(v)?;
			Ok((k, v))
		})
		.collect::<Result<Vec<(CborValue, CborValue)>, &str>>()
		.map(CborValue::Map)
}

fn to_object(obj: Vec<(CborValue, CborValue)>) -> Result<Object, &'static str> {
	let res = obj
		.into_iter()
		.map(|(k, v)| {
			let CborValue::Text(k) = k else {
				return Err("Expected object key to be a string");
			};
			let v = to_value(v)?;
			Ok((k, v))
		})
		.collect::<Result<BTreeMap<_, _>, &str>>()?;
	Ok(Object(res))
}
