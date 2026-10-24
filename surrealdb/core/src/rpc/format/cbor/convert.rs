use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::iter::once;
use std::ops::Bound;

use anyhow::{Context, Result, anyhow};
use ciborium::Value as CborValue;
use common::decimal::DecimalExt;
use geo_types::{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};
use rust_decimal::Decimal;

use crate::syn;
use crate::types::{
	PublicArray, PublicDatetime, PublicDuration, PublicFile, PublicGeometry, PublicNumber,
	PublicObject, PublicRange, PublicRecordId, PublicRecordIdKey, PublicRecordIdKeyRange,
	PublicSet, PublicTable, PublicUuid, PublicValue,
};

// Tags from the spec - https://www.iana.org/assignments/cbor-tags/cbor-tags.xhtml
const TAG_SPEC_UUID: u64 = 37;

// Custom tags (6->15 is unassigned)
const TAG_NONE: u64 = 6;
const TAG_TABLE: u64 = 7;
const TAG_RECORDID: u64 = 8;
const TAG_STRING_DECIMAL: u64 = 10;
// const TAG_BINARY_DECIMAL: u64 = 11;
const TAG_CUSTOM_DATETIME: u64 = 12;
const TAG_CUSTOM_DURATION: u64 = 14;
// unused but the for backwards compatibility kept around to maybe avoid using
// that tag again.
const _TAG_FUTURE: u64 = 15;

// Ranges (49->51 is unassigned)
const TAG_RANGE: u64 = 49;
const TAG_BOUND_INCLUDED: u64 = 50;
const TAG_BOUND_EXCLUDED: u64 = 51;

// A shaped block, accepted in requests only (`decode_request`)
const TAG_SHAPED_BLOCK: u64 = surrealdb_types::shaped::CBOR_TAG;

// Sized number tags (IANA unassigned 180..=188)
pub const TAG_I8: u64 = 180;
pub const TAG_U8: u64 = 181;
pub const TAG_I16: u64 = 182;
pub const TAG_U16: u64 = 183;
pub const TAG_I32: u64 = 184;
pub const TAG_U32: u64 = 185;
pub const TAG_U64: u64 = 186;
pub const TAG_F16: u64 = 187;
pub const TAG_F32: u64 = 188;

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

fn value(val: CborValue, ctx: &mut Ctx) -> Result<PublicValue> {
	match val {
		CborValue::Null => Ok(PublicValue::Null),
		CborValue::Bool(v) => Ok(PublicValue::Bool(v)),
		CborValue::Integer(v) => Ok(PublicValue::Number(PublicNumber::Int(to_i64(v)?))),
		CborValue::Float(v) => Ok(PublicValue::Number(PublicNumber::Float(v))),
		CborValue::Bytes(v) => Ok(PublicValue::Bytes(v.into())),
		CborValue::Text(v) => Ok(PublicValue::String(v)),
		CborValue::Array(v) => Ok(PublicValue::Array(to_array(v, ctx)?)),
		CborValue::Map(v) => Ok(PublicValue::Object(to_object(v, ctx)?)),
		CborValue::Tag(t, v) => {
			#[cfg(feature = "shaped")]
			if t == TAG_SHAPED_BLOCK && ctx.blocks.is_some() {
				return block(*v, ctx);
			}
			let ctx = &mut ctx.child();
			match t {
				// A custom [seconds: i64, nanos: u32] datetime
				TAG_CUSTOM_DATETIME => {
					if let CborValue::Array(v) = *v
						&& v.len() == 2
					{
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
					} else {
						Err(anyhow!("Expected a CBOR array with 2 elements"))
					}
				}
				// A literal NONE
				TAG_NONE => {
					if let CborValue::Null = *v {
						Ok(PublicValue::None)
					} else {
						Err(anyhow!("Expected a CBOR null under the NONE tag"))
					}
				}
				// A byte string uuid
				TAG_SPEC_UUID => to_uuid(*v).map(PublicValue::Uuid),
				// A literal decimal
				TAG_STRING_DECIMAL => {
					if let CborValue::Text(v) = *v {
						if let Ok(v) = Decimal::from_str_normalized(v.as_str()) {
							Ok(PublicValue::Number(PublicNumber::Decimal(v)))
						} else {
							Err(anyhow!("Expected a valid Decimal value"))
						}
					} else {
						Err(anyhow!("Expected a CBOR text data type"))
					}
				}
				// A custom [seconds: Option<u64>, nanos: Option<u32>] duration
				TAG_CUSTOM_DURATION => {
					if let CborValue::Array(v) = *v
						&& v.len() <= 2
					{
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

						let seconds = seconds
							.checked_add(u64::from(nanos / 1_000_000_000))
							.ok_or_else(|| anyhow!("Expected a valid Duration value"))?;

						Ok(PublicValue::Duration(PublicDuration::new(
							seconds,
							nanos % 1_000_000_000,
						)))
					} else {
						Err(anyhow!("Expected a CBOR array with at most 2 elements"))
					}
				}
				// A literal recordid
				TAG_RECORDID => {
					if let CborValue::Text(v) = *v {
						if let Ok(v) = syn::record_id(v.as_str()) {
							Ok(PublicValue::RecordId(v))
						} else {
							Err(anyhow!("Expected a valid RecordID value"))
						}
					} else if let CborValue::Array(v) = *v {
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

						let table = sub(table, ctx)?.into_string()?;

						let key = to_record_id_key(key, &mut ctx.child())?;

						Ok(PublicValue::RecordId(PublicRecordId {
							table: table.into(),
							key,
						}))
					} else {
						Err(anyhow!(
							"Expected a CBOR text data type, or a CBOR array with 2 elements"
						))
					}
				}
				// A literal table
				TAG_TABLE => {
					if let CborValue::Text(v) = *v {
						Ok(PublicValue::Table(PublicTable::new(v)))
					} else {
						Err(anyhow!("Expected a CBOR text data type"))
					}
				}
				// A range
				TAG_RANGE => Ok(PublicValue::Range(Box::new(to_range(*v, ctx)?))),
				TAG_GEOMETRY_POINT => {
					if let CborValue::Array(v) = *v {
						let err_msg = "Expected a CBOR array with 2 decimal values";
						let mut iter = v.into_iter();
						let x = iter.next().ok_or_else(|| anyhow!(err_msg))?;
						let y = iter.next().ok_or_else(|| anyhow!(err_msg))?;
						if iter.next().is_some() {
							return Err(anyhow!(err_msg));
						};

						let x = sub(x, ctx)?;
						let y = sub(y, ctx)?;

						if let (PublicValue::Number(x), PublicValue::Number(y)) = (x, y) {
							Ok(PublicValue::Geometry(PublicGeometry::Point(
								(x.to_f64().unwrap_or_default(), y.to_f64().unwrap_or_default())
									.into(),
							)))
						} else {
							Err(anyhow!("Expected a CBOR array with 2 decimal values"))
						}
					} else {
						Err(anyhow!("Expected a CBOR array with 2 decimal values"))
					}
				}
				TAG_GEOMETRY_LINE => {
					if let CborValue::Array(v) = *v {
						let points = v
							.into_iter()
							.map(|v| {
								if let PublicValue::Geometry(PublicGeometry::Point(v)) =
									sub(v, ctx)?
								{
									Ok(v)
								} else {
									Err(anyhow!("Expected a CBOR array with Geometry Point values"))
								}
							})
							.collect::<Result<Vec<Point>>>()?;

						Ok(PublicValue::Geometry(PublicGeometry::Line(LineString::from(points))))
					} else {
						Err(anyhow!("Expected a CBOR array with Geometry Point values"))
					}
				}
				TAG_GEOMETRY_POLYGON => {
					if let CborValue::Array(v) = *v
						&& !v.is_empty()
					{
						let lines = v
							.into_iter()
							.map(|v| {
								if let PublicValue::Geometry(PublicGeometry::Line(v)) = sub(v, ctx)?
								{
									Ok(v)
								} else {
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
					} else {
						Err(anyhow!("Expected a CBOR array with at least one Geometry Line values"))
					}
				}
				TAG_GEOMETRY_MULTIPOINT => {
					if let CborValue::Array(v) = *v {
						let points = v
							.into_iter()
							.map(|v| {
								if let PublicValue::Geometry(PublicGeometry::Point(v)) =
									sub(v, ctx)?
								{
									Ok(v)
								} else {
									Err(anyhow!("Expected a CBOR array with Geometry Point values"))
								}
							})
							.collect::<Result<Vec<Point>>>()?;

						Ok(PublicValue::Geometry(PublicGeometry::MultiPoint(MultiPoint::from(
							points,
						))))
					} else {
						Err(anyhow!("Expected a CBOR array with Geometry Point values"))
					}
				}
				TAG_GEOMETRY_MULTILINE => {
					if let CborValue::Array(v) = *v {
						let lines = v
							.into_iter()
							.map(|v| {
								if let PublicValue::Geometry(PublicGeometry::Line(v)) = sub(v, ctx)?
								{
									Ok(v)
								} else {
									Err(anyhow!("Expected a CBOR array with Geometry Line values"))
								}
							})
							.collect::<Result<Vec<LineString>>>()?;

						Ok(PublicValue::Geometry(PublicGeometry::MultiLine(MultiLineString::new(
							lines,
						))))
					} else {
						Err(anyhow!("Expected a CBOR array with Geometry Line values"))
					}
				}
				TAG_GEOMETRY_MULTIPOLYGON => {
					if let CborValue::Array(v) = *v {
						let polygons = v
							.into_iter()
							.map(|v| {
								if let PublicValue::Geometry(PublicGeometry::Polygon(v)) =
									sub(v, ctx)?
								{
									Ok(v)
								} else {
									Err(anyhow!(
										"Expected a CBOR array with Geometry Polygon values"
									))
								}
							})
							.collect::<Result<Vec<Polygon>>>()?;

						Ok(PublicValue::Geometry(PublicGeometry::MultiPolygon(MultiPolygon::from(
							polygons,
						))))
					} else {
						Err(anyhow!("Expected a CBOR array with Geometry Polygon values"))
					}
				}
				TAG_GEOMETRY_COLLECTION => {
					if let CborValue::Array(v) = *v {
						let geometries = v
							.into_iter()
							.map(|v| {
								if let PublicValue::Geometry(v) = sub(v, ctx)? {
									Ok(v)
								} else {
									Err(anyhow!("Expected a CBOR array with Geometry values"))
								}
							})
							.collect::<Result<Vec<PublicGeometry>>>()?;

						Ok(PublicValue::Geometry(PublicGeometry::Collection(geometries)))
					} else {
						Err(anyhow!("Expected a CBOR array with Geometry values"))
					}
				}
				TAG_FILE => {
					if let CborValue::Array(mut v) = *v
						&& v.len() == 2
					{
						let CborValue::Text(bucket) = v.remove(0) else {
							return Err(anyhow!("Expected the bucket name to be a string value"));
						};

						let CborValue::Text(key) = v.remove(0) else {
							return Err(anyhow!("Expected the file key to be a string value"));
						};

						Ok(PublicValue::File(PublicFile::new(bucket, key)))
					} else {
						Err(anyhow!("Expected a CBOR array with two String bucket and key values"))
					}
				}
				TAG_SET => {
					if let CborValue::Array(v) = *v {
						Ok(PublicValue::Set(PublicSet::from(
							v.into_iter()
								.map(|v| sub(v, ctx))
								.collect::<Result<BTreeSet<PublicValue>>>()?,
						)))
					} else {
						Err(anyhow!("Expected a CBOR array with Set values"))
					}
				}
				TAG_I8 if ctx.v2 => {
					if let CborValue::Integer(i) = *v {
						Ok(PublicValue::Number(PublicNumber::I8(to_i8(i)?)))
					} else {
						Err(anyhow!("Expected a CBOR integer under tag {TAG_I8}"))
					}
				}
				TAG_U8 if ctx.v2 => {
					if let CborValue::Integer(i) = *v {
						Ok(PublicValue::Number(PublicNumber::U8(to_u8(i)?)))
					} else {
						Err(anyhow!("Expected a CBOR integer under tag {TAG_U8}"))
					}
				}
				TAG_I16 if ctx.v2 => {
					if let CborValue::Integer(i) = *v {
						Ok(PublicValue::Number(PublicNumber::I16(to_i16(i)?)))
					} else {
						Err(anyhow!("Expected a CBOR integer under tag {TAG_I16}"))
					}
				}
				TAG_U16 if ctx.v2 => {
					if let CborValue::Integer(i) = *v {
						Ok(PublicValue::Number(PublicNumber::U16(to_u16(i)?)))
					} else {
						Err(anyhow!("Expected a CBOR integer under tag {TAG_U16}"))
					}
				}
				TAG_I32 if ctx.v2 => {
					if let CborValue::Integer(i) = *v {
						Ok(PublicValue::Number(PublicNumber::I32(to_i32(i)?)))
					} else {
						Err(anyhow!("Expected a CBOR integer under tag {TAG_I32}"))
					}
				}
				TAG_U32 if ctx.v2 => {
					if let CborValue::Integer(i) = *v {
						Ok(PublicValue::Number(PublicNumber::U32(to_u32(i)?)))
					} else {
						Err(anyhow!("Expected a CBOR integer under tag {TAG_U32}"))
					}
				}
				TAG_U64 if ctx.v2 => {
					if let CborValue::Integer(i) = *v {
						Ok(PublicValue::Number(PublicNumber::U64(to_u64(i)?)))
					} else {
						Err(anyhow!("Expected a CBOR integer under tag {TAG_U64}"))
					}
				}
				TAG_F16 if ctx.v2 => {
					if let CborValue::Float(f) = *v {
						Ok(PublicValue::Number(PublicNumber::F16(to_f16(f)?)))
					} else {
						Err(anyhow!("Expected a CBOR float under tag {TAG_F16}"))
					}
				}
				TAG_F32 if ctx.v2 => {
					if let CborValue::Float(f) = *v {
						Ok(PublicValue::Number(PublicNumber::F32(to_f32(f)?)))
					} else {
						Err(anyhow!("Expected a CBOR float under tag {TAG_F32}"))
					}
				}
				// A tag this format does not read
				t => Err(unsupported_tag(t)),
			}
		}
		_ => Err(anyhow!("Encountered an unknown CBOR data type")),
	}
}

/// The refusal of a tag the format does not read: the tags that carried a datetime, uuid or
/// duration as text name the form to send instead.
fn unsupported_tag(tag: u64) -> anyhow::Error {
	match tag {
		0 => anyhow!(
			"CBOR tag 0 (a datetime as text) is not accepted: send tag {TAG_CUSTOM_DATETIME} [seconds, nanos]"
		),
		9 => anyhow!(
			"CBOR tag 9 (a uuid as text) is not accepted: send tag {TAG_SPEC_UUID} (16 bytes)"
		),
		13 => anyhow!(
			"CBOR tag 13 (a duration as text) is not accepted: send tag {TAG_CUSTOM_DURATION} [seconds, nanos]"
		),
		TAG_SHAPED_BLOCK => anyhow!("CBOR tag 57 (a shaped block) is not accepted here"),
		TAG_I8..=TAG_F32 => anyhow!("CBOR tag {tag} is not accepted here: use cbor.2"),
		_ => anyhow!("Encountered an unknown CBOR tag"),
	}
}

/// What a request's shaped blocks may materialise: the budget they share and the recursion limit
/// the request is decoded under.
#[cfg_attr(not(feature = "shaped"), allow(dead_code))]
pub struct RequestBlocks {
	pub budget: surrealdb_types::shaped::Budget,
	pub limit: usize,
}

/// Where an item sits in the value being decoded, and whether it may be a shaped block.
pub struct Ctx<'a> {
	blocks: Option<&'a mut RequestBlocks>,
	/// The number of arrays, maps and tags around the item.
	depth: usize,
	pub v2: bool,
}

impl<'a> Ctx<'a> {
	/// The context of an item whose blocks are refused.
	fn strict() -> Ctx<'static> {
		Ctx {
			blocks: None,
			depth: 0,
			v2: false,
		}
	}

	fn strict_v2() -> Ctx<'static> {
		Ctx {
			blocks: None,
			depth: 0,
			v2: true,
		}
	}

	/// The context of a value that accepts blocks within `blocks`.
	#[cfg_attr(not(feature = "shaped"), allow(dead_code))]
	pub fn requests(blocks: &'a mut RequestBlocks, v2: bool) -> Self {
		Ctx {
			blocks: Some(blocks),
			depth: 0,
			v2,
		}
	}

	/// The context of an item one level in.
	fn child(&mut self) -> Ctx<'_> {
		Ctx {
			blocks: self.blocks.as_deref_mut(),
			depth: self.depth + 1,
			v2: self.v2,
		}
	}
}

/// An item one level inside the one `ctx` describes.
fn sub(val: CborValue, ctx: &mut Ctx) -> Result<PublicValue> {
	value(val, &mut ctx.child())
}

pub fn to_value(val: CborValue) -> Result<PublicValue> {
	value(val, &mut Ctx::strict())
}

pub fn to_value_v2(val: CborValue) -> Result<PublicValue> {
	value(val, &mut Ctx::strict_v2())
}

/// A decoded request item, where a shaped block is the array of its rows.
#[cfg_attr(not(feature = "shaped"), allow(dead_code))]
pub fn to_request_value(val: CborValue, ctx: &mut Ctx) -> Result<PublicValue> {
	value(val, ctx)
}

/// A shaped block at the depth `ctx` holds: the array of its rows, read in a scope of its own.
///
/// Every row is a value a plain CBOR request could carry, and the block can nest no deeper than
/// the request's recursion limit: the rows add an array and an object around the shape's own
/// object columns, and a dynamic slice decodes within what is left.
#[cfg(feature = "shaped")]
fn block(val: CborValue, ctx: &mut Ctx) -> Result<PublicValue> {
	let CborValue::Bytes(contents) = val else {
		return Err(anyhow!("a shaped block that is not a byte string"));
	};
	let depth = ctx.depth;
	let Some(blocks) = ctx.blocks.as_deref_mut() else {
		return Err(unsupported_tag(TAG_SHAPED_BLOCK));
	};
	let limit = blocks.limit;
	// The rows' array and object sit inside the block's own item.
	let room = limit.checked_sub(depth + 2).filter(|room| *room > 0);
	blocks.budget.max_shape_depth = match room {
		Some(room) => room - 1,
		None => return Err(anyhow!("a shaped block nested too deep")),
	};
	let v2 = ctx.v2;
	let mut dynamic = |slice: &[u8], shape_depth: usize| -> Result<PublicValue, String> {
		let room = limit
			.checked_sub(depth + 2 + shape_depth)
			.filter(|room| *room > 0)
			.ok_or_else(|| "a shaped block nested too deep".to_owned())?;
		if v2 {
			super::decode_exact_v2(slice, room).map_err(|e| e.to_string())
		} else {
			super::decode_exact(slice, room).map_err(|e| e.to_string())
		}
	};
	let rows = surrealdb_types::shaped::BlockReader::new()
		.read_block_within(&contents, &mut dynamic, &mut blocks.budget)
		.map_err(|e| anyhow!(e))?;
	Ok(PublicValue::Array(rows.into()))
}

pub fn from_value(val: PublicValue) -> Result<CborValue> {
	from_value_inner(val, false)
}

pub fn from_value_v2(val: PublicValue) -> Result<CborValue> {
	from_value_inner(val, true)
}

fn from_value_inner(val: PublicValue, v2: bool) -> Result<CborValue> {
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
			PublicNumber::I8(v) => {
				if v2 {
					Ok(CborValue::Tag(TAG_I8, Box::new(CborValue::Integer(i64::from(v).into()))))
				} else {
					Ok(CborValue::Integer(i64::from(v).into()))
				}
			}
			PublicNumber::U8(v) => {
				if v2 {
					Ok(CborValue::Tag(TAG_U8, Box::new(CborValue::Integer(i64::from(v).into()))))
				} else {
					Ok(CborValue::Integer(i64::from(v).into()))
				}
			}
			PublicNumber::I16(v) => {
				if v2 {
					Ok(CborValue::Tag(TAG_I16, Box::new(CborValue::Integer(i64::from(v).into()))))
				} else {
					Ok(CborValue::Integer(i64::from(v).into()))
				}
			}
			PublicNumber::U16(v) => {
				if v2 {
					Ok(CborValue::Tag(TAG_U16, Box::new(CborValue::Integer(i64::from(v).into()))))
				} else {
					Ok(CborValue::Integer(i64::from(v).into()))
				}
			}
			PublicNumber::I32(v) => {
				if v2 {
					Ok(CborValue::Tag(TAG_I32, Box::new(CborValue::Integer(i64::from(v).into()))))
				} else {
					Ok(CborValue::Integer(i64::from(v).into()))
				}
			}
			PublicNumber::U32(v) => {
				if v2 {
					Ok(CborValue::Tag(TAG_U32, Box::new(CborValue::Integer(i64::from(v).into()))))
				} else {
					Ok(CborValue::Integer(i64::from(v).into()))
				}
			}
			PublicNumber::U64(v) => {
				if v2 {
					Ok(CborValue::Tag(TAG_U64, Box::new(CborValue::Integer(v.into()))))
				} else if v <= i64::MAX as u64 {
					Ok(CborValue::Integer(v.into()))
				} else {
					Ok(CborValue::Tag(TAG_STRING_DECIMAL, Box::new(CborValue::Text(v.to_string()))))
				}
			}
			PublicNumber::F16(v) => {
				if v2 {
					Ok(CborValue::Tag(TAG_F16, Box::new(CborValue::Float(v.to_f64()))))
				} else {
					Ok(CborValue::Float(v.to_f64()))
				}
			}
			PublicNumber::F32(v) => {
				if v2 {
					Ok(CborValue::Tag(TAG_F32, Box::new(CborValue::Float(f64::from(v)))))
				} else {
					Ok(CborValue::Float(f64::from(v)))
				}
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
		PublicValue::Array(v) => from_array(v, v2),
		PublicValue::Object(v) => from_object(v, v2),
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
					PublicRecordIdKey::Array(v) => from_array(v, v2)?,
					PublicRecordIdKey::Object(v) => from_object(v, v2)?,
					PublicRecordIdKey::Range(v) => {
						CborValue::Tag(TAG_RANGE, Box::new(from_record_id_key_range(*v, v2)?))
					}
				},
			])),
		)),
		PublicValue::Geometry(v) => from_geometry(v),
		PublicValue::Range(v) => Ok(CborValue::Tag(TAG_RANGE, Box::new(from_range(*v, v2)?))),
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
				v.into_iter()
					.map(|item| from_value_inner(item, v2))
					.collect::<Result<Vec<CborValue>>>()?,
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

fn to_range(val: CborValue, ctx: &mut Ctx) -> Result<PublicRange> {
	fn decode_bound(v: CborValue, ctx: &mut Ctx) -> Result<Bound<PublicValue>> {
		if let CborValue::Tag(TAG_BOUND_INCLUDED, v) = v {
			Ok(Bound::Included(sub(*v, ctx)?))
		} else if let CborValue::Tag(TAG_BOUND_EXCLUDED, v) = v {
			Ok(Bound::Excluded(sub(*v, ctx)?))
		} else if let CborValue::Null = v {
			Ok(Bound::Unbounded)
		} else {
			Err(anyhow!("Expected a bound tag"))
		}
	}

	if let CborValue::Array(mut v) = val
		&& v.len() == 2
	{
		let beg = decode_bound(v.remove(0), &mut ctx.child())?;
		let end = decode_bound(v.remove(0), &mut ctx.child())?;
		Ok(PublicRange::new(beg, end))
	} else {
		Err(anyhow!("Expected a CBOR array with 2 bounds"))
	}
}

fn from_range(r: PublicRange, v2: bool) -> Result<CborValue> {
	let (start, end) = r.into_inner();
	Ok(CborValue::Array(vec![encode_bound(start, v2)?, encode_bound(end, v2)?]))
}

fn encode_bound(b: Bound<PublicValue>, v2: bool) -> Result<CborValue> {
	match b {
		Bound::Included(v) => {
			Ok(CborValue::Tag(TAG_BOUND_INCLUDED, Box::new(from_value_inner(v, v2)?)))
		}
		Bound::Excluded(v) => {
			Ok(CborValue::Tag(TAG_BOUND_EXCLUDED, Box::new(from_value_inner(v, v2)?)))
		}
		Bound::Unbounded => Ok(CborValue::Null),
	}
}

fn from_record_id_key_range(r: PublicRecordIdKeyRange, v2: bool) -> Result<CborValue> {
	let (start, end) = r.into_inner();
	Ok(CborValue::Array(vec![encode_key_bound(start, v2)?, encode_key_bound(end, v2)?]))
}

fn encode_key_bound(b: Bound<PublicRecordIdKey>, v2: bool) -> Result<CborValue> {
	match b {
		Bound::Included(v) => {
			Ok(CborValue::Tag(TAG_BOUND_INCLUDED, Box::new(from_record_id_key(v, v2)?)))
		}
		Bound::Excluded(v) => {
			Ok(CborValue::Tag(TAG_BOUND_EXCLUDED, Box::new(from_record_id_key(v, v2)?)))
		}
		Bound::Unbounded => Ok(CborValue::Null),
	}
}

fn to_record_id_key_range(val: CborValue, ctx: &mut Ctx) -> Result<PublicRecordIdKeyRange> {
	fn decode_bound(v: CborValue, ctx: &mut Ctx) -> Result<Bound<PublicRecordIdKey>> {
		if let CborValue::Tag(TAG_BOUND_INCLUDED, v) = v {
			Ok(Bound::Included(to_record_id_key(*v, &mut ctx.child())?))
		} else if let CborValue::Tag(TAG_BOUND_EXCLUDED, v) = v {
			Ok(Bound::Excluded(to_record_id_key(*v, &mut ctx.child())?))
		} else if let CborValue::Null = v {
			Ok(Bound::Unbounded)
		} else {
			Err(anyhow!("Expected a bound tag"))
		}
	}

	if let CborValue::Array(mut v) = val
		&& v.len() == 2
	{
		let start = decode_bound(v.remove(0), &mut ctx.child())?;
		let end = decode_bound(v.remove(0), &mut ctx.child())?;

		Ok(PublicRecordIdKeyRange {
			start,
			end,
		})
	} else {
		Err(anyhow!("Expected a CBOR array with 2 bounds"))
	}
}

fn from_record_id_key(v: PublicRecordIdKey, v2: bool) -> Result<CborValue> {
	match v {
		PublicRecordIdKey::Number(v) => Ok(CborValue::Integer(v.into())),
		PublicRecordIdKey::String(v) => Ok(CborValue::Text(v)),
		PublicRecordIdKey::Array(v) => from_array(v, v2),
		PublicRecordIdKey::Object(v) => from_object(v, v2),
		PublicRecordIdKey::Range(v) => {
			Ok(CborValue::Tag(TAG_RANGE, Box::new(from_record_id_key_range(*v, v2)?)))
		}
		PublicRecordIdKey::Uuid(v) => {
			Ok(CborValue::Tag(TAG_SPEC_UUID, Box::new(CborValue::Bytes(v.into_bytes().into()))))
		}
	}
}

fn to_record_id_key(val: CborValue, ctx: &mut Ctx) -> Result<PublicRecordIdKey> {
	match val {
		CborValue::Integer(v) => Ok(PublicRecordIdKey::Number(to_i64(v)?)),
		CborValue::Text(v) => Ok(PublicRecordIdKey::String(v)),
		CborValue::Array(v) => Ok(PublicRecordIdKey::Array(to_array(v, ctx)?)),
		CborValue::Map(v) => Ok(PublicRecordIdKey::Object(to_object(v, ctx)?)),
		CborValue::Tag(TAG_RANGE, v) => {
			Ok(PublicRecordIdKey::Range(Box::new(to_record_id_key_range(*v, &mut ctx.child())?)))
		}
		CborValue::Tag(TAG_SPEC_UUID, v) => to_uuid(*v).map(PublicRecordIdKey::Uuid),
		CborValue::Tag(TAG_I8, v) if ctx.v2 => {
			if let CborValue::Integer(i) = *v {
				Ok(PublicRecordIdKey::Number(i64::from(to_i8(i)?)))
			} else {
				Err(anyhow!("Expected a CBOR integer under tag {TAG_I8}"))
			}
		}
		CborValue::Tag(TAG_U8, v) if ctx.v2 => {
			if let CborValue::Integer(i) = *v {
				Ok(PublicRecordIdKey::Number(i64::from(to_u8(i)?)))
			} else {
				Err(anyhow!("Expected a CBOR integer under tag {TAG_U8}"))
			}
		}
		CborValue::Tag(TAG_I16, v) if ctx.v2 => {
			if let CborValue::Integer(i) = *v {
				Ok(PublicRecordIdKey::Number(i64::from(to_i16(i)?)))
			} else {
				Err(anyhow!("Expected a CBOR integer under tag {TAG_I16}"))
			}
		}
		CborValue::Tag(TAG_U16, v) if ctx.v2 => {
			if let CborValue::Integer(i) = *v {
				Ok(PublicRecordIdKey::Number(i64::from(to_u16(i)?)))
			} else {
				Err(anyhow!("Expected a CBOR integer under tag {TAG_U16}"))
			}
		}
		CborValue::Tag(TAG_I32, v) if ctx.v2 => {
			if let CborValue::Integer(i) = *v {
				Ok(PublicRecordIdKey::Number(i64::from(to_i32(i)?)))
			} else {
				Err(anyhow!("Expected a CBOR integer under tag {TAG_I32}"))
			}
		}
		CborValue::Tag(TAG_U32, v) if ctx.v2 => {
			if let CborValue::Integer(i) = *v {
				Ok(PublicRecordIdKey::Number(i64::from(to_u32(i)?)))
			} else {
				Err(anyhow!("Expected a CBOR integer under tag {TAG_U32}"))
			}
		}
		CborValue::Tag(TAG_U64, v) if ctx.v2 => {
			if let CborValue::Integer(i) = *v {
				let n = to_u64(i)?;
				let signed = i64::try_from(n)
					.map_err(|_| anyhow!("integer {n} is outside the 64-bit signed range"))?;
				Ok(PublicRecordIdKey::Number(signed))
			} else {
				Err(anyhow!("Expected a CBOR integer under tag {TAG_U64}"))
			}
		}
		CborValue::Tag(t @ (0 | 9 | 13 | TAG_I8..=TAG_F32), _) => Err(unsupported_tag(t)),
		CborValue::Bytes(_)
		| CborValue::Float(_)
		| CborValue::Bool(_)
		| CborValue::Null
		| CborValue::Tag(..) => Err(anyhow!("Expected a CBOR integer, text, array or map")),
		_ => Err(anyhow!("Expected a CBOR integer, text, array or map")),
	}
}

fn from_uuid(val: PublicUuid) -> CborValue {
	CborValue::Tag(TAG_SPEC_UUID, Box::new(CborValue::Bytes(val.into_bytes().into())))
}

fn to_uuid(val: CborValue) -> Result<PublicUuid> {
	if let CborValue::Bytes(v) = val
		&& v.len() == 16
	{
		if let Ok(v) = v.as_slice().try_into() {
			Ok(PublicUuid::from(uuid::Uuid::from_bytes(v)))
		} else {
			Err(anyhow!("Expected a CBOR byte array with 16 elements"))
		}
	} else {
		Err(anyhow!("Expected a CBOR byte array with 16 elements"))
	}
}

fn from_array(array: PublicArray, v2: bool) -> Result<CborValue> {
	array
		.into_iter()
		.map(|v| from_value_inner(v, v2))
		.collect::<Result<Vec<CborValue>>>()
		.map(CborValue::Array)
}

/// A CBOR integer as the `i64` every integer value holds; one outside that range is refused
/// rather than wrapped.
fn to_i64(v: ciborium::value::Integer) -> Result<i64> {
	i64::try_from(v)
		.map_err(|_| anyhow!("integer {} is outside the 64-bit signed range", i128::from(v)))
}

fn to_i8(v: ciborium::value::Integer) -> Result<i8> {
	let n = i128::from(v);
	i8::try_from(n).map_err(|_| anyhow!("integer {n} is outside the 8-bit signed range"))
}

fn to_u8(v: ciborium::value::Integer) -> Result<u8> {
	let n = i128::from(v);
	u8::try_from(n).map_err(|_| anyhow!("integer {n} is outside the 8-bit unsigned range"))
}

fn to_i16(v: ciborium::value::Integer) -> Result<i16> {
	let n = i128::from(v);
	i16::try_from(n).map_err(|_| anyhow!("integer {n} is outside the 16-bit signed range"))
}

fn to_u16(v: ciborium::value::Integer) -> Result<u16> {
	let n = i128::from(v);
	u16::try_from(n).map_err(|_| anyhow!("integer {n} is outside the 16-bit unsigned range"))
}

fn to_i32(v: ciborium::value::Integer) -> Result<i32> {
	let n = i128::from(v);
	i32::try_from(n).map_err(|_| anyhow!("integer {n} is outside the 32-bit signed range"))
}

fn to_u32(v: ciborium::value::Integer) -> Result<u32> {
	let n = i128::from(v);
	u32::try_from(n).map_err(|_| anyhow!("integer {n} is outside the 32-bit unsigned range"))
}

fn to_u64(v: ciborium::value::Integer) -> Result<u64> {
	let n = i128::from(v);
	u64::try_from(n).map_err(|_| anyhow!("integer {n} is outside the 64-bit unsigned range"))
}

fn to_f16(f: f64) -> Result<surrealdb_types::f16> {
	let val = surrealdb_types::f16::from_f64(f);
	if f.is_nan() || val.to_f64().to_bits() == f.to_bits() {
		Ok(val)
	} else {
		Err(anyhow!("float {f} cannot be losslessly represented as f16"))
	}
}

fn to_f32(f: f64) -> Result<f32> {
	let val = f as f32;
	if f.is_nan() || (val as f64).to_bits() == f.to_bits() {
		Ok(val)
	} else {
		Err(anyhow!("float {f} cannot be losslessly represented as f32"))
	}
}

fn to_array(array: Vec<CborValue>, ctx: &mut Ctx) -> Result<PublicArray> {
	Ok(array.into_iter().map(|v| sub(v, ctx)).collect::<Result<Vec<PublicValue>, _>>()?.into())
}

fn from_object(obj: PublicObject, v2: bool) -> Result<CborValue> {
	obj.into_iter()
		.map(|(k, v)| {
			let k = CborValue::Text(k);
			let v = from_value_inner(v, v2)?;
			Ok((k, v))
		})
		.collect::<Result<Vec<(CborValue, CborValue)>>>()
		.map(CborValue::Map)
}

fn to_object(obj: Vec<(CborValue, CborValue)>, ctx: &mut Ctx) -> Result<PublicObject> {
	let mut res = BTreeMap::new();
	for (k, v) in obj {
		let CborValue::Text(k) = k else {
			return Err(anyhow!("Expected object key to be a string"));
		};
		let v = sub(v, ctx)?;
		match res.entry(k) {
			Entry::Vacant(slot) => {
				slot.insert(v);
			}
			Entry::Occupied(slot) => {
				return Err(anyhow!("duplicate key {} in a CBOR map", slot.key()));
			}
		}
	}
	Ok(PublicObject::from(res))
}
