//! The values of the PostgreSQL types a declared SQL type names (ADR 0014 D4 to D7) in both wire
//! formats: `inet`, `cidr`, `macaddr`, `macaddr8`, `bit`, `varbit`, `money`, `pg_lsn`, the
//! geometric types, and the ranges and multiranges.
//!
//! Each type is stored as an existing kind: the network, MAC and bit-string types as their
//! canonical text, `money` as a decimal of two fractional digits, `pg_lsn` as a `u64`, `point`,
//! `lseg`, `path`, `box`, and `polygon` as geometries, `line` and `circle` as objects of a fixed
//! shape, a range as a range, and a multirange as an array of ranges. This module maps a stored
//! value to the plain values the codecs take and back. The text forms are the declared types' own
//! ([`text_form`]), shared with the write path and the SQL front end; the binary forms are the
//! wire's ([`crate::codec`]).
//!
//! A value of another shape in a column of one of these types is refused, except a string, which
//! is taken as the type's text: the text format sends it as it is and the binary format reads it
//! first.

use std::ops::Bound;

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use surrealdb_sqlplan::sql_types::text_form::{
	geometry as geo_text, lsn as lsn_text, money as money_text, range as range_text,
};
use surrealdb_sqlplan::sql_types::text_shaped::{format_mac, network, parse_mac};
use surrealdb_types::{Array, Geometry, Number, Object, Value};

use crate::codec::{
	bits, geometry as geo_binary, lsn, mac, money, network as net_binary, oid, range,
};
use crate::encode::{self, mismatch};
use crate::error::PgError;
use crate::typing::PgType;

/// A coordinate pair, as the geometric codecs take it.
type Point = geo_text::Point;

/// The geometric types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Shape {
	Point,
	Lseg,
	Path,
	Box,
	Polygon,
	Line,
	Circle,
}

/// The types this module encodes and decodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Family {
	Inet,
	Cidr,
	MacAddr,
	MacAddr8,
	Bit,
	Varbit,
	Money,
	PgLsn,
	/// A 32-bit identifier (`regproc` and the other `reg*` aliases, `xid`, `cid`), or the 64-bit
	/// `xid8` (`wide`).
	Identifier {
		wide: bool,
	},
	Geometric(Shape),
	/// A range (`false`) or multirange (`true`) of the element type.
	Range(PgType, bool),
}

impl Family {
	/// The family of a wire type, or `None` for a type this module does not own.
	pub(crate) fn of(ty: &PgType) -> Option<Self> {
		if let Some((element, multi)) = ty.range_element() {
			return Some(Self::Range(element_type(element), multi));
		}
		Some(match ty {
			PgType::Inet => Self::Inet,
			PgType::Cidr => Self::Cidr,
			PgType::MacAddr => Self::MacAddr,
			PgType::Macaddr8 => Self::MacAddr8,
			PgType::Bit => Self::Bit,
			PgType::Varbit => Self::Varbit,
			PgType::Money => Self::Money,
			PgType::PgLsn => Self::PgLsn,
			PgType::Point => Self::Geometric(Shape::Point),
			PgType::Lseg => Self::Geometric(Shape::Lseg),
			PgType::Path => Self::Geometric(Shape::Path),
			PgType::PgBox => Self::Geometric(Shape::Box),
			PgType::Polygon => Self::Geometric(Shape::Polygon),
			PgType::Line => Self::Geometric(Shape::Line),
			PgType::Circle => Self::Geometric(Shape::Circle),
			PgType::Regproc
			| PgType::Regprocedure
			| PgType::Regoper
			| PgType::Regoperator
			| PgType::Regtype
			| PgType::Regnamespace
			| PgType::Regrole
			| PgType::Regconfig
			| PgType::Regdictionary
			| PgType::Regcollation
			| PgType::Xid
			| PgType::Cid => Self::Identifier {
				wide: false,
			},
			PgType::Xid8 => Self::Identifier {
				wide: true,
			},
			PgType::Bool
			| PgType::Bytea
			| PgType::Char
			| PgType::Name
			| PgType::Int8
			| PgType::Int2
			| PgType::Int4
			| PgType::Text
			| PgType::Oid
			| PgType::Json
			| PgType::Float4
			| PgType::Float8
			| PgType::Bpchar
			| PgType::Varchar
			| PgType::Date
			| PgType::Time
			| PgType::Timestamp
			| PgType::Timestamptz
			| PgType::Interval
			| PgType::Timetz
			| PgType::Numeric
			| PgType::Regclass
			| PgType::Record
			| PgType::Uuid
			| PgType::Jsonb
			| PgType::Void
			| PgType::Vector
			| PgType::Xml
			| PgType::Tsvector
			| PgType::Tsquery
			| PgType::Jsonpath
			| PgType::Tid
			| PgType::PgSnapshot
			| PgType::TxidSnapshot
			| PgType::Int4range
			| PgType::Numrange
			| PgType::Tsrange
			| PgType::Tstzrange
			| PgType::Daterange
			| PgType::Int8range
			| PgType::Int4multirange
			| PgType::Nummultirange
			| PgType::Tsmultirange
			| PgType::Tstzmultirange
			| PgType::Datemultirange
			| PgType::Int8multirange
			| PgType::Enum(_)
			| PgType::Composite {
				..
			}
			| PgType::Array(_) => return None,
		})
	}
}

/// The wire type of a range's element.
fn element_type(element: surrealdb_sqlplan::RangeElement) -> PgType {
	use surrealdb_sqlplan::RangeElement;
	match element {
		RangeElement::Int4 => PgType::Int4,
		RangeElement::Int8 => PgType::Int8,
		RangeElement::Numeric => PgType::Numeric,
		RangeElement::Timestamp => PgType::Timestamp,
		RangeElement::Timestamptz => PgType::Timestamptz,
		RangeElement::Date => PgType::Date,
	}
}

fn invalid(ty: &PgType, detail: impl std::fmt::Display) -> PgError {
	PgError::invalid_text(format!("invalid value for type oid {}: {detail}", ty.oid()))
}

/// The text form of `value` in a column of the family's type `ty`.
pub(crate) fn encode_text(family: &Family, value: Value, ty: &PgType) -> Result<Vec<u8>, PgError> {
	if let Value::String(text) = value {
		return Ok(text.into_bytes());
	}
	let text = match family {
		Family::Inet
		| Family::Cidr
		| Family::MacAddr
		| Family::MacAddr8
		| Family::Bit
		| Family::Varbit => return Err(mismatch(&value, ty)),
		Family::Money => money_text::money_text(cents(&value).ok_or_else(|| mismatch(&value, ty))?),
		Family::PgLsn => lsn_text::lsn_text(lsn_of(&value).ok_or_else(|| mismatch(&value, ty))?),
		Family::Identifier {
			wide,
		} => identifier(&value, *wide).ok_or_else(|| mismatch(&value, ty))?.to_string(),
		Family::Geometric(shape) => {
			let plain = geometric(*shape, &value).ok_or_else(|| mismatch(&value, ty))?;
			plain.text()
		}
		Family::Range(element, multi) => {
			if *multi {
				let ranges = multirange_of(&value).ok_or_else(|| mismatch(&value, ty))?;
				let ranges = ranges
					.into_iter()
					.map(|range| text_bounds(range, element))
					.collect::<Result<Vec<_>, PgError>>()?;
				range_text::encode_multirange_text(&ranges, String::clone)
			} else {
				let range = range_of(&value).ok_or_else(|| mismatch(&value, ty))?;
				range_text::encode_text(&text_bounds(range, element)?, String::clone)
			}
		}
	};
	Ok(text.into_bytes())
}

/// The binary form of `value` in a column of the family's type `ty`. A string is read as the
/// type's text first.
pub(crate) fn encode_binary(
	family: &Family,
	value: Value,
	ty: &PgType,
) -> Result<Vec<u8>, PgError> {
	let value = if let Value::String(text) = value {
		match family {
			Family::Inet
			| Family::Cidr
			| Family::MacAddr
			| Family::MacAddr8
			| Family::Bit
			| Family::Varbit => Value::String(text),
			Family::Money
			| Family::PgLsn
			| Family::Identifier {
				..
			}
			| Family::Geometric(_)
			| Family::Range(..) => decode_text(family, &text, ty)?,
		}
	} else {
		value
	};
	match family {
		Family::Inet | Family::Cidr => {
			let Value::String(text) = &value else {
				return Err(mismatch(&value, ty));
			};
			let net = network::parse(text).map_err(|error| invalid(ty, error))?;
			let octets = &net.address[..net.family.octets()];
			net_binary::encode(octets, net.bits, *family == Family::Cidr)
				.map_err(|error| invalid(ty, error))
		}
		Family::MacAddr | Family::MacAddr8 => {
			let Value::String(text) = &value else {
				return Err(mismatch(&value, ty));
			};
			let width = if *family == Family::MacAddr {
				6
			} else {
				8
			};
			parse_mac(text, width).ok_or_else(|| invalid(ty, format!("not a MAC address: {text}")))
		}
		Family::Bit | Family::Varbit => {
			let Value::String(text) = &value else {
				return Err(mismatch(&value, ty));
			};
			bits::encode(text).map_err(|error| invalid(ty, error))
		}
		Family::Money => {
			Ok(money::money_binary(cents(&value).ok_or_else(|| mismatch(&value, ty))?).to_vec())
		}
		Family::PgLsn => {
			Ok(lsn::lsn_binary(lsn_of(&value).ok_or_else(|| mismatch(&value, ty))?).to_vec())
		}
		Family::Identifier {
			wide,
		} => {
			let id = identifier(&value, *wide).ok_or_else(|| mismatch(&value, ty))?;
			if *wide {
				return Ok(id.to_be_bytes().to_vec());
			}
			let id = u32::try_from(id).map_err(|_| mismatch(&value, ty))?;
			Ok(oid::oid_binary(id).to_vec())
		}
		Family::Geometric(shape) => {
			let plain = geometric(*shape, &value).ok_or_else(|| mismatch(&value, ty))?;
			Ok(plain.binary())
		}
		Family::Range(element, multi) => {
			if *multi {
				let ranges = multirange_of(&value).ok_or_else(|| mismatch(&value, ty))?;
				let ranges = ranges
					.into_iter()
					.map(|range| binary_bounds(range, element))
					.collect::<Result<Vec<_>, PgError>>()?;
				range::encode_multirange_binary(&ranges, Vec::clone)
					.map_err(|error| invalid(ty, error.message))
			} else {
				let range = range_of(&value).ok_or_else(|| mismatch(&value, ty))?;
				range::encode_binary(&binary_bounds(range, element)?, Vec::clone)
					.map_err(|error| invalid(ty, error.message))
			}
		}
	}
}

/// The value a text parameter of the family's type reads as. A network, MAC or bit-string text
/// is kept as written, for the write path to check and canonicalise.
pub(crate) fn decode_text(family: &Family, text: &str, ty: &PgType) -> Result<Value, PgError> {
	match family {
		Family::Inet
		| Family::Cidr
		| Family::MacAddr
		| Family::MacAddr8
		| Family::Bit
		| Family::Varbit => Ok(Value::String(text.to_string())),
		Family::Money => money_text::parse_money_text(text)
			.map(|cents| Value::Number(Number::Decimal(Decimal::new(cents, 2))))
			.ok_or_else(|| invalid(ty, format!("not a money amount: {text}"))),
		Family::PgLsn => lsn_text::parse_lsn_text(text)
			.map(|lsn| Value::Number(Number::U64(lsn)))
			.ok_or_else(|| invalid(ty, format!("not a pg_lsn: {text}"))),
		Family::Identifier {
			wide: false,
		} => text
			.trim()
			.parse::<u32>()
			.map(|id| Value::Number(Number::U32(id)))
			.map_err(|error| invalid(ty, error)),
		Family::Identifier {
			wide: true,
		} => text
			.trim()
			.parse::<u64>()
			.map(|id| Value::Number(Number::U64(id)))
			.map_err(|error| invalid(ty, error)),
		Family::Geometric(shape) => {
			Plain::parse(*shape, text).map(Plain::value).map_err(|error| invalid(ty, error))
		}
		Family::Range(element, multi) => {
			let element_oid = element.oid();
			let parse = |bound: &str| encode::decode_text(bound, element_oid).ok();
			if *multi {
				range_text::decode_multirange_text(text, parse)
					.map(|ranges| multirange_value(ranges, element))
					.map_err(|error| invalid(ty, error.message))
			} else {
				range_text::decode_text(text, parse)
					.map(|range| range_value(range, element))
					.map_err(|error| invalid(ty, error.message))
			}
		}
	}
}

/// The value a binary parameter of the family's type reads as.
pub(crate) fn decode_binary(family: &Family, bytes: &[u8], ty: &PgType) -> Result<Value, PgError> {
	match family {
		Family::Inet | Family::Cidr => {
			let net = net_binary::decode(bytes).map_err(|error| invalid(ty, error))?;
			// The header's own type flag is ignored, as PostgreSQL ignores it and clients leave it
			// clear; a `cidr` still has no bits set past its prefix, which the text parse checks.
			let cidr = *family == Family::Cidr;
			let written = format!(
				"{}/{}",
				address_text(&net.address).ok_or_else(|| invalid(ty, "bad address"))?,
				net.bits
			);
			let text = if cidr {
				network::parse_cidr(&written)
			} else {
				network::parse(&written)
			}
			.map_err(|error| invalid(ty, error))?;
			Ok(Value::String(if *family == Family::Cidr {
				text.cidr_text()
			} else {
				text.inet_text()
			}))
		}
		Family::MacAddr => mac::decode_macaddr(bytes)
			.map(|octets| Value::String(format_mac(&octets)))
			.map_err(|error| invalid(ty, error)),
		Family::MacAddr8 => mac::decode_macaddr8(bytes)
			.map(|octets| Value::String(format_mac(&octets)))
			.map_err(|error| invalid(ty, error)),
		Family::Bit | Family::Varbit => {
			bits::decode(bytes).map(Value::String).map_err(|error| invalid(ty, error))
		}
		Family::Money => money::money_from_binary(bytes)
			.map(|cents| Value::Number(Number::Decimal(Decimal::new(cents, 2))))
			.ok_or_else(|| invalid(ty, "a money value is 8 bytes")),
		Family::PgLsn => lsn::lsn_from_binary(bytes)
			.map(|lsn| Value::Number(Number::U64(lsn)))
			.ok_or_else(|| invalid(ty, "a pg_lsn value is 8 bytes")),
		Family::Identifier {
			wide: false,
		} => oid::oid_from_binary(bytes)
			.map(|id| Value::Number(Number::U32(id)))
			.ok_or_else(|| invalid(ty, "a 32-bit identifier is 4 bytes")),
		Family::Identifier {
			wide: true,
		} => <[u8; 8]>::try_from(bytes)
			.map(|raw| Value::Number(Number::U64(u64::from_be_bytes(raw))))
			.map_err(|_| invalid(ty, "an xid8 is 8 bytes")),
		Family::Geometric(shape) => {
			Plain::decode(*shape, bytes).map(Plain::value).map_err(|error| invalid(ty, error))
		}
		Family::Range(element, multi) => {
			let element_oid = element.oid();
			let parse = |bound: &[u8]| encode::decode_binary(bound, element_oid).ok();
			if *multi {
				range::decode_multirange_binary(bytes, parse)
					.map(|ranges| multirange_value(ranges, element))
					.map_err(|error| invalid(ty, error.message))
			} else {
				range::decode_binary(bytes, parse)
					.map(|range| range_value(range, element))
					.map_err(|error| invalid(ty, error.message))
			}
		}
	}
}

/// The text of the address octets of a binary network value: four octets of IPv4 or sixteen of
/// IPv6.
fn address_text(octets: &[u8]) -> Option<String> {
	match octets.len() {
		4 => {
			let array: [u8; 4] = octets.try_into().ok()?;
			Some(std::net::Ipv4Addr::from(array).to_string())
		}
		16 => {
			let array: [u8; 16] = octets.try_into().ok()?;
			Some(std::net::Ipv6Addr::from(array).to_string())
		}
		_ => None,
	}
}

/// The cents of a `money` amount: the number rounded half away from zero to two fractional
/// digits, as the type stores it.
fn cents(value: &Value) -> Option<i64> {
	let Value::Number(number) = value else {
		return None;
	};
	let amount = match number {
		Number::Decimal(decimal) => *decimal,
		Number::BigDecimal(big) => Decimal::try_from(big.as_ref()).ok()?,
		Number::Float(_) | Number::F16(_) | Number::F32(_) => return None,
		Number::Int(_)
		| Number::I8(_)
		| Number::U8(_)
		| Number::I16(_)
		| Number::U16(_)
		| Number::I32(_)
		| Number::U32(_)
		| Number::U64(_) => Decimal::from(integer(number)?),
	};
	let scaled = amount.checked_mul(Decimal::from(100))?;
	scaled.round_dp_with_strategy(0, rust_decimal::RoundingStrategy::MidpointAwayFromZero).to_i64()
}

/// The integer a whole number holds.
fn integer(number: &Number) -> Option<i128> {
	Some(match number {
		Number::Int(n) => i128::from(*n),
		Number::I8(n) => i128::from(*n),
		Number::U8(n) => i128::from(*n),
		Number::I16(n) => i128::from(*n),
		Number::U16(n) => i128::from(*n),
		Number::I32(n) => i128::from(*n),
		Number::U32(n) => i128::from(*n),
		Number::U64(n) => i128::from(*n),
		Number::Decimal(_)
		| Number::BigDecimal(_)
		| Number::Float(_)
		| Number::F16(_)
		| Number::F32(_) => return None,
	})
}

/// The identifier a number holds: within 32 bits unless `wide`.
fn identifier(value: &Value, wide: bool) -> Option<u64> {
	let Value::Number(number) = value else {
		return None;
	};
	let id = integer(number)?;
	if wide {
		u64::try_from(id).ok()
	} else {
		u32::try_from(id).ok().map(u64::from)
	}
}

fn lsn_of(value: &Value) -> Option<u64> {
	let Value::Number(number) = value else {
		return None;
	};
	u64::try_from(integer(number)?).ok()
}

/// A geometric value as the codecs take it.
#[derive(Clone, Debug, PartialEq)]
enum Plain {
	Point(Point),
	Lseg(Point, Point),
	Path(Vec<Point>, bool),
	Box(Point, Point),
	Polygon(Vec<Point>),
	Line(f64, f64, f64),
	Circle(Point, f64),
}

impl Plain {
	fn text(&self) -> String {
		match self {
			Self::Point(point) => geo_text::format_point(*point),
			Self::Lseg(start, end) => geo_text::format_lseg(*start, *end),
			Self::Path(points, closed) => geo_text::format_path(points, *closed),
			Self::Box(high, low) => geo_text::format_box(*high, *low),
			Self::Polygon(points) => geo_text::format_polygon(points),
			Self::Line(a, b, c) => geo_text::format_line(*a, *b, *c),
			Self::Circle(center, radius) => geo_text::format_circle(*center, *radius),
		}
	}

	fn binary(&self) -> Vec<u8> {
		match self {
			Self::Point(point) => geo_binary::encode_point_binary(*point),
			Self::Lseg(start, end) => geo_binary::encode_lseg_binary(*start, *end),
			Self::Path(points, closed) => geo_binary::encode_path_binary(points, *closed),
			Self::Box(high, low) => geo_binary::encode_box_binary(*high, *low),
			Self::Polygon(points) => geo_binary::encode_polygon_binary(points),
			Self::Line(a, b, c) => geo_binary::encode_line_binary(*a, *b, *c),
			Self::Circle(center, radius) => geo_binary::encode_circle_binary(*center, *radius),
		}
	}

	fn parse(shape: Shape, text: &str) -> anyhow::Result<Self> {
		Ok(match shape {
			Shape::Point => Self::Point(geo_text::parse_point(text)?),
			Shape::Lseg => {
				let (start, end) = geo_text::parse_lseg(text)?;
				Self::Lseg(start, end)
			}
			Shape::Path => {
				let (points, closed) = geo_text::parse_path(text)?;
				Self::Path(points, closed)
			}
			Shape::Box => {
				let (first, second) = geo_text::parse_box(text)?;
				Self::boxed(first, second)
			}
			Shape::Polygon => Self::Polygon(geo_text::parse_polygon(text)?),
			Shape::Line => {
				let (a, b, c) = geo_text::parse_line(text)?;
				Self::Line(a, b, c)
			}
			Shape::Circle => {
				let (center, radius) = geo_text::parse_circle(text)?;
				Self::Circle(center, radius)
			}
		})
	}

	fn decode(shape: Shape, bytes: &[u8]) -> anyhow::Result<Self> {
		Ok(match shape {
			Shape::Point => Self::Point(geo_binary::decode_point_binary(bytes)?),
			Shape::Lseg => {
				let (start, end) = geo_binary::decode_lseg_binary(bytes)?;
				Self::Lseg(start, end)
			}
			Shape::Path => {
				let (points, closed) = geo_binary::decode_path_binary(bytes)?;
				Self::Path(points, closed)
			}
			Shape::Box => {
				let (first, second) = geo_binary::decode_box_binary(bytes)?;
				Self::boxed(first, second)
			}
			Shape::Polygon => Self::Polygon(geo_binary::decode_polygon_binary(bytes)?),
			Shape::Line => {
				let (a, b, c) = geo_binary::decode_line_binary(bytes)?;
				Self::Line(a, b, c)
			}
			Shape::Circle => {
				let (center, radius) = geo_binary::decode_circle_binary(bytes)?;
				Self::Circle(center, radius)
			}
		})
	}

	/// A box from two opposite corners in either order: the high corner first.
	fn boxed(first: Point, second: Point) -> Self {
		Self::Box(
			(first.0.max(second.0), first.1.max(second.1)),
			(first.0.min(second.0), first.1.min(second.1)),
		)
	}

	/// The stored value: a geometry, or an object for `line` and `circle`. A box is the polygon
	/// ring of its corners from the high corner, as the write path stores it.
	fn value(self) -> Value {
		match self {
			Self::Point(point) => Value::Geometry(Geometry::Point(point.into())),
			Self::Lseg(start, end) => Value::Geometry(Geometry::Line(vec![start, end].into())),
			Self::Path(points, false) => Value::Geometry(Geometry::Line(points.into())),
			Self::Path(points, true) | Self::Polygon(points) => polygon_value(&points),
			Self::Box(high, low) => {
				polygon_value(&[high, (low.0, high.1), low, (high.0, low.1), high])
			}
			Self::Line(a, b, c) => fixed_object(&[("a", a), ("b", b), ("c", c)]),
			Self::Circle((x, y), r) => fixed_object(&[("x", x), ("y", y), ("r", r)]),
		}
	}
}

/// A polygon of one ring through `points`, closed as a ring is.
fn polygon_value(points: &[Point]) -> Value {
	let mut ring: Vec<Value> = points.iter().map(|(x, y)| coordinate(*x, *y)).collect();
	if let (Some(first), Some(last)) = (points.first(), points.last())
		&& first != last
	{
		ring.push(coordinate(first.0, first.1));
	}
	let mut object = Object::new();
	object.insert("type".to_string(), Value::String("Polygon".to_string()));
	object.insert(
		"coordinates".to_string(),
		Value::Array(Array::from(vec![Value::Array(Array::from(ring))])),
	);
	match Geometry::try_from_object(&object) {
		Some(geometry) => Value::Geometry(geometry),
		None => Value::Object(object),
	}
}

fn coordinate(x: f64, y: f64) -> Value {
	Value::Array(Array::from(vec![
		Value::Number(Number::Float(x)),
		Value::Number(Number::Float(y)),
	]))
}

fn fixed_object(fields: &[(&str, f64)]) -> Value {
	let mut object = Object::new();
	for (key, value) in fields {
		object.insert((*key).to_string(), Value::Number(Number::Float(*value)));
	}
	Value::Object(object)
}

/// The geometric value a stored value of `shape` holds.
fn geometric(shape: Shape, value: &Value) -> Option<Plain> {
	match shape {
		Shape::Point => {
			let Value::Geometry(Geometry::Point(point)) = value else {
				return None;
			};
			Some(Plain::Point((point.x(), point.y())))
		}
		Shape::Lseg => {
			let Value::Geometry(Geometry::Line(line)) = value else {
				return None;
			};
			match line.0.as_slice() {
				[start, end] => Some(Plain::Lseg((start.x, start.y), (end.x, end.y))),
				_ => None,
			}
		}
		Shape::Path => {
			if let Value::Geometry(Geometry::Line(line)) = value {
				Some(Plain::Path(line.0.iter().map(|c| (c.x, c.y)).collect(), false))
			} else if let Value::Geometry(Geometry::Polygon(polygon)) = value {
				let ring = polygon.exterior().0.iter().map(|c| (c.x, c.y)).collect();
				Some(Plain::Path(open_ring(ring), true))
			} else {
				None
			}
		}
		Shape::Polygon => {
			let Value::Geometry(Geometry::Polygon(polygon)) = value else {
				return None;
			};
			let ring = polygon.exterior().0.iter().map(|c| (c.x, c.y)).collect();
			Some(Plain::Polygon(open_ring(ring)))
		}
		Shape::Box => {
			let Value::Geometry(Geometry::Polygon(polygon)) = value else {
				return None;
			};
			let mut corners = polygon.exterior().0.iter().map(|c| (c.x, c.y));
			let first = corners.next()?;
			let (high, low) = corners.fold((first, first), |(high, low), (x, y)| {
				((high.0.max(x), high.1.max(y)), (low.0.min(x), low.1.min(y)))
			});
			Some(Plain::Box(high, low))
		}
		Shape::Line => {
			let Value::Object(object) = value else {
				return None;
			};
			let [a, b, c] = fixed_fields(object, ["a", "b", "c"])?;
			Some(Plain::Line(a, b, c))
		}
		Shape::Circle => {
			let Value::Object(object) = value else {
				return None;
			};
			let [x, y, r] = fixed_fields(object, ["x", "y", "r"])?;
			Some(Plain::Circle((x, y), r))
		}
	}
}

/// The points of a ring without the closing point it repeats.
fn open_ring(mut points: Vec<Point>) -> Vec<Point> {
	if points.len() > 1 && points.first() == points.last() {
		points.pop();
	}
	points
}

fn fixed_fields<const N: usize>(object: &Object, keys: [&str; N]) -> Option<[f64; N]> {
	if object.len() != N {
		return None;
	}
	let mut out = [0.0; N];
	for (slot, key) in out.iter_mut().zip(keys) {
		match object.get(key) {
			Some(Value::Number(number)) => *slot = number.to_f64()?,
			_ => return None,
		}
	}
	Some(out)
}

/// The range a value holds.
fn range_of(value: &Value) -> Option<range::Range<Value>> {
	let Value::Range(range) = value else {
		return None;
	};
	Some(span(range.start.clone(), range.end.clone()))
}

/// The ranges of a multirange: an array of ranges, or a single range.
fn multirange_of(value: &Value) -> Option<Vec<range::Range<Value>>> {
	if let Value::Array(items) = value {
		return items.iter().map(range_of).collect();
	}
	range_of(value).map(|range| vec![range])
}

/// A span, or the empty range when the bounds hold no value: equal bounds that are not both
/// inclusive, or a lower bound above the upper.
fn span(lower: Bound<Value>, upper: Bound<Value>) -> range::Range<Value> {
	let empty = match (&lower, &upper) {
		(Bound::Included(lo), Bound::Included(hi)) => lo > hi,
		(Bound::Included(lo) | Bound::Excluded(lo), Bound::Excluded(hi))
		| (Bound::Excluded(lo), Bound::Included(hi)) => lo >= hi,
		(Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
	};
	if empty {
		range::Range::Empty
	} else {
		range::Range::Span {
			lower,
			upper,
		}
	}
}

fn map_bounds<T>(
	range: range::Range<Value>,
	mut f: impl FnMut(Value) -> Result<T, PgError>,
) -> Result<range::Range<T>, PgError> {
	let mut bound = |bound: Bound<Value>| -> Result<Bound<T>, PgError> {
		Ok(match bound {
			Bound::Included(value) => Bound::Included(f(value)?),
			Bound::Excluded(value) => Bound::Excluded(f(value)?),
			Bound::Unbounded => Bound::Unbounded,
		})
	};
	Ok(match range {
		range::Range::Empty => range::Range::Empty,
		range::Range::Span {
			lower,
			upper,
		} => range::Range::Span {
			lower: bound(lower)?,
			upper: bound(upper)?,
		},
	})
}

fn text_bounds(
	range: range::Range<Value>,
	element: &PgType,
) -> Result<range::Range<String>, PgError> {
	map_bounds(range, |value| {
		let bytes = encode::encode_text(value, element)?;
		String::from_utf8(bytes).map_err(|_| PgError::internal("a range bound is not UTF-8"))
	})
}

fn binary_bounds(
	range: range::Range<Value>,
	element: &PgType,
) -> Result<range::Range<Vec<u8>>, PgError> {
	map_bounds(range, |value| encode::encode_binary(value, element))
}

fn range_value(range: range::Range<Value>, element: &PgType) -> Value {
	let (start, end) = match range {
		range::Range::Span {
			lower,
			upper,
		} => (lower, upper),
		// An empty range is equal bounds with an exclusive end, at the element's zero.
		range::Range::Empty => {
			let zero = element_zero(element);
			(Bound::Included(zero.clone()), Bound::Excluded(zero))
		}
	};
	Value::Range(Box::new(surrealdb_types::Range::new(start, end)))
}

fn multirange_value(ranges: Vec<range::Range<Value>>, element: &PgType) -> Value {
	Value::Array(Array::from(
		ranges.into_iter().map(|range| range_value(range, element)).collect::<Vec<Value>>(),
	))
}

/// The value an empty range of the element is stored at.
fn element_zero(element: &PgType) -> Value {
	let text = if *element == PgType::Timestamp {
		"1970-01-01 00:00:00"
	} else if *element == PgType::Timestamptz {
		"1970-01-01 00:00:00+00"
	} else if *element == PgType::Date {
		"1970-01-01"
	} else {
		"0"
	};
	encode::decode_text(text, element.oid()).unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests;
