#![deny(clippy::wildcard_enum_match_arm)]

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use common::fmt::{EscapeKwFreeIdent, EscapeObjectKey, Float, Fmt, QuoteStr, SqlDuration};
use geo_types::{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};
use revision::revisioned;
use rust_decimal::Decimal;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Literal, Part, Value};
use crate::val::table_name_public::{IntoPublicTable, IntoTableName};
use crate::val::{
	Array, Bytes, Closure, Date, Datetime, Duration, File, Geometry, Interval, Number, Range,
	RecordId, Regex, Set, TableName, Time, Timestamp, Uuid,
};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum GeometryKind {
	Point,
	Line,
	Polygon,
	MultiPoint,
	MultiLine,
	MultiPolygon,
	Collection,
}

impl ToSql for GeometryKind {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		match self {
			GeometryKind::Point => f.push_str("point"),
			GeometryKind::Line => f.push_str("line"),
			GeometryKind::Polygon => f.push_str("polygon"),
			GeometryKind::MultiPoint => f.push_str("multipoint"),
			GeometryKind::MultiLine => f.push_str("multiline"),
			GeometryKind::MultiPolygon => f.push_str("multipolygon"),
			GeometryKind::Collection => f.push_str("collection"),
		}
	}
}

impl FromStr for GeometryKind {
	type Err = anyhow::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"point" => Ok(GeometryKind::Point),
			"line" => Ok(GeometryKind::Line),
			"polygon" => Ok(GeometryKind::Polygon),
			"multipoint" => Ok(GeometryKind::MultiPoint),
			"multiline" => Ok(GeometryKind::MultiLine),
			"multipolygon" => Ok(GeometryKind::MultiPolygon),
			"collection" => Ok(GeometryKind::Collection),
			_ => Err(anyhow::anyhow!("invalid geometry kind: {s}")),
		}
	}
}

impl From<GeometryKind> for crate::types::PublicGeometryKind {
	fn from(k: GeometryKind) -> Self {
		match k {
			GeometryKind::Point => crate::types::PublicGeometryKind::Point,
			GeometryKind::Line => crate::types::PublicGeometryKind::Line,
			GeometryKind::Polygon => crate::types::PublicGeometryKind::Polygon,
			GeometryKind::MultiPoint => crate::types::PublicGeometryKind::MultiPoint,
			GeometryKind::MultiLine => crate::types::PublicGeometryKind::MultiLine,
			GeometryKind::MultiPolygon => crate::types::PublicGeometryKind::MultiPolygon,
			GeometryKind::Collection => crate::types::PublicGeometryKind::Collection,
		}
	}
}

impl From<crate::types::PublicGeometryKind> for GeometryKind {
	fn from(k: crate::types::PublicGeometryKind) -> Self {
		match k {
			crate::types::PublicGeometryKind::Point => GeometryKind::Point,
			crate::types::PublicGeometryKind::Line => GeometryKind::Line,
			crate::types::PublicGeometryKind::Polygon => GeometryKind::Polygon,
			crate::types::PublicGeometryKind::MultiPoint => GeometryKind::MultiPoint,
			crate::types::PublicGeometryKind::MultiLine => GeometryKind::MultiLine,
			crate::types::PublicGeometryKind::MultiPolygon => GeometryKind::MultiPolygon,
			crate::types::PublicGeometryKind::Collection => GeometryKind::Collection,
		}
	}
}

/// The kind, or data type, of a value or field.
#[revisioned(revision = 2, serialize = false)]
#[derive(Debug, Default, Eq, PartialEq, surrealdb_stored_derive::DynHash)]
pub enum Kind {
	/// The most generic type, can be anything.
	#[default]
	Any,
	/// None type.
	None,
	/// Null type.
	Null,
	/// Boolean type.
	Bool,
	/// Bytes type.
	Bytes,
	/// Datetime type.
	Datetime,
	/// Decimal type.
	Decimal,
	/// Duration type.
	Duration,
	/// 64-bit floating point type.
	Float,
	/// 64-bit signed integer type.
	Int,
	/// Number type, can be either a float, int or decimal.
	/// This is the most generic type for numbers.
	Number,
	/// Object type.
	Object,
	/// String type.
	String,
	/// UUID type.
	Uuid,
	/// Regular expression type.
	Regex,
	/// A table type.
	Table(Vec<TableName>),
	/// A record type.
	Record(Vec<TableName>),
	/// A geometry type.
	/// The vec contains the geometry types as strings, for example `"point"` or
	/// `"polygon"`.
	Geometry(Vec<GeometryKind>),
	/// An either type.
	/// Can be any of the kinds in the vec.
	Either(Vec<Kind>),
	/// A set type.
	Set(Box<Kind>, Option<u64>),
	/// An array type.
	Array(Box<Kind>, Option<u64>),
	/// A function type.
	/// The first option is the argument types, the second is the optional
	/// return type.
	Function(Option<Vec<Kind>>, Option<Box<Kind>>),
	/// A range type.
	Range,
	/// A literal type.
	/// The literal type is used to represent a type that can only be a single
	/// value. For example, `"a"` is a literal type which can only ever be
	/// `"a"`. This can be used in the `Kind::Either` type to represent an
	/// enum.
	Literal(KindLiteral),
	/// A file type.
	/// If the kind was specified without a bucket the vec will be empty.
	/// So `<file>` is just `Kind::File(Vec::new())`
	File(Vec<String>),
	#[revision(start = 2)]
	I8,
	#[revision(start = 2)]
	U8,
	#[revision(start = 2)]
	I16,
	#[revision(start = 2)]
	U16,
	#[revision(start = 2)]
	I32,
	#[revision(start = 2)]
	U32,
	#[revision(start = 2)]
	U64,
	#[revision(start = 2)]
	F16,
	#[revision(start = 2)]
	F32,
	#[revision(start = 2)]
	Date,
	#[revision(start = 2)]
	Time,
	#[revision(start = 2)]
	Timestamp,
	#[revision(start = 2)]
	Interval,
	/// An inline enum type: a string constrained to an ordered set of labels, in declaration
	/// order.
	#[revision(start = 2)]
	Enum(Vec<String>),
	/// An arbitrary-precision decimal number
	BigDecimal,
	/// A type declared by `DEFINE TYPE`, by name. What it admits is the catalog's to say: a value
	/// coerces to it unchanged, and the write path, a cast and a function call judge the value
	/// against the definition the database holds.
	#[revision(start = 2)]
	Named(String),
}

impl ::revision::SerializeRevisioned for Kind {
	fn serialize_revisioned<W: ::std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), ::revision::Error> {
		match self {
			Kind::Any => {
				1u16.serialize_revisioned(writer)?;
				0u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::None => {
				1u16.serialize_revisioned(writer)?;
				1u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::Null => {
				1u16.serialize_revisioned(writer)?;
				2u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::Bool => {
				1u16.serialize_revisioned(writer)?;
				3u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::Bytes => {
				1u16.serialize_revisioned(writer)?;
				4u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::Datetime => {
				1u16.serialize_revisioned(writer)?;
				5u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::Decimal => {
				1u16.serialize_revisioned(writer)?;
				6u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::Duration => {
				1u16.serialize_revisioned(writer)?;
				7u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::Float => {
				1u16.serialize_revisioned(writer)?;
				8u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::Int => {
				1u16.serialize_revisioned(writer)?;
				9u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::Number => {
				1u16.serialize_revisioned(writer)?;
				10u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::Object => {
				1u16.serialize_revisioned(writer)?;
				11u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::String => {
				1u16.serialize_revisioned(writer)?;
				12u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::Uuid => {
				1u16.serialize_revisioned(writer)?;
				13u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::Regex => {
				1u16.serialize_revisioned(writer)?;
				14u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::Table(v) => {
				1u16.serialize_revisioned(writer)?;
				15u32.serialize_revisioned(writer)?;
				v.serialize_revisioned(writer)
			}
			Kind::Record(v) => {
				1u16.serialize_revisioned(writer)?;
				16u32.serialize_revisioned(writer)?;
				v.serialize_revisioned(writer)
			}
			Kind::Geometry(v) => {
				1u16.serialize_revisioned(writer)?;
				17u32.serialize_revisioned(writer)?;
				v.serialize_revisioned(writer)
			}
			Kind::Either(v) => {
				1u16.serialize_revisioned(writer)?;
				18u32.serialize_revisioned(writer)?;
				v.serialize_revisioned(writer)
			}
			Kind::Set(v0, v1) => {
				1u16.serialize_revisioned(writer)?;
				19u32.serialize_revisioned(writer)?;
				v0.serialize_revisioned(writer)?;
				v1.serialize_revisioned(writer)
			}
			Kind::Array(v0, v1) => {
				1u16.serialize_revisioned(writer)?;
				20u32.serialize_revisioned(writer)?;
				v0.serialize_revisioned(writer)?;
				v1.serialize_revisioned(writer)
			}
			Kind::Function(v0, v1) => {
				1u16.serialize_revisioned(writer)?;
				21u32.serialize_revisioned(writer)?;
				v0.serialize_revisioned(writer)?;
				v1.serialize_revisioned(writer)
			}
			Kind::Range => {
				1u16.serialize_revisioned(writer)?;
				22u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::Literal(v) => {
				1u16.serialize_revisioned(writer)?;
				23u32.serialize_revisioned(writer)?;
				v.serialize_revisioned(writer)
			}
			Kind::File(v) => {
				1u16.serialize_revisioned(writer)?;
				24u32.serialize_revisioned(writer)?;
				v.serialize_revisioned(writer)
			}
			Kind::I8 => {
				2u16.serialize_revisioned(writer)?;
				25u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::U8 => {
				2u16.serialize_revisioned(writer)?;
				26u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::I16 => {
				2u16.serialize_revisioned(writer)?;
				27u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::U16 => {
				2u16.serialize_revisioned(writer)?;
				28u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::I32 => {
				2u16.serialize_revisioned(writer)?;
				29u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::U32 => {
				2u16.serialize_revisioned(writer)?;
				30u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::U64 => {
				2u16.serialize_revisioned(writer)?;
				31u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::F16 => {
				2u16.serialize_revisioned(writer)?;
				32u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::F32 => {
				2u16.serialize_revisioned(writer)?;
				33u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::Date => {
				2u16.serialize_revisioned(writer)?;
				34u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::Time => {
				2u16.serialize_revisioned(writer)?;
				35u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::Timestamp => {
				2u16.serialize_revisioned(writer)?;
				36u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::Interval => {
				2u16.serialize_revisioned(writer)?;
				37u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::BigDecimal => {
				2u16.serialize_revisioned(writer)?;
				39u32.serialize_revisioned(writer)?;
				Ok(())
			}
			Kind::Enum(v) => {
				2u16.serialize_revisioned(writer)?;
				38u32.serialize_revisioned(writer)?;
				v.serialize_revisioned(writer)
			}
			Kind::Named(v) => {
				2u16.serialize_revisioned(writer)?;
				40u32.serialize_revisioned(writer)?;
				v.serialize_revisioned(writer)
			}
		}
	}
}

/// One body for every clone of a kind, whichever crate clones it: a derived
/// `clone` is `#[inline]`, so each crate that clones a kind, or a `Box` of
/// one, would carry its own copy of the whole match.
impl Clone for Kind {
	#[inline]
	fn clone(&self) -> Self {
		clone_kind(self)
	}
}

#[inline(never)]
fn clone_kind(kind: &Kind) -> Kind {
	match kind {
		Kind::Any => Kind::Any,
		Kind::None => Kind::None,
		Kind::Null => Kind::Null,
		Kind::Bool => Kind::Bool,
		Kind::Bytes => Kind::Bytes,
		Kind::Datetime => Kind::Datetime,
		Kind::Decimal => Kind::Decimal,
		Kind::BigDecimal => Kind::BigDecimal,
		Kind::Duration => Kind::Duration,
		Kind::Float => Kind::Float,
		Kind::Int => Kind::Int,
		Kind::Number => Kind::Number,
		Kind::Object => Kind::Object,
		Kind::String => Kind::String,
		Kind::Uuid => Kind::Uuid,
		Kind::Regex => Kind::Regex,
		Kind::Table(tables) => Kind::Table(tables.clone()),
		Kind::Record(tables) => Kind::Record(tables.clone()),
		Kind::Geometry(kinds) => Kind::Geometry(kinds.clone()),
		Kind::Either(kinds) => Kind::Either(kinds.clone()),
		Kind::Set(kind, max) => Kind::Set(kind.clone(), *max),
		Kind::Array(kind, max) => Kind::Array(kind.clone(), *max),
		Kind::Function(args, ret) => Kind::Function(args.clone(), ret.clone()),
		Kind::Range => Kind::Range,
		Kind::Literal(literal) => Kind::Literal(literal.clone()),
		Kind::File(buckets) => Kind::File(buckets.clone()),
		Kind::I8 => Kind::I8,
		Kind::U8 => Kind::U8,
		Kind::I16 => Kind::I16,
		Kind::U16 => Kind::U16,
		Kind::I32 => Kind::I32,
		Kind::U32 => Kind::U32,
		Kind::U64 => Kind::U64,
		Kind::F16 => Kind::F16,
		Kind::F32 => Kind::F32,
		Kind::Date => Kind::Date,
		Kind::Time => Kind::Time,
		Kind::Timestamp => Kind::Timestamp,
		Kind::Interval => Kind::Interval,
		Kind::Enum(labels) => Kind::Enum(labels.clone()),
		Kind::Named(name) => Kind::Named(name.clone()),
	}
}

/// The value variants a kind's coercion accepts, as a bit set: what two kinds
/// are disjoint by. Over-approximate by design: a kind whose accepted values
/// are not known exactly claims every variant.
mod coerce_domain {
	pub const NONE: u32 = 1 << 0;
	pub const NULL: u32 = 1 << 1;
	pub const BOOL: u32 = 1 << 2;
	pub const NUMBER: u32 = 1 << 3;
	pub const STRING: u32 = 1 << 4;
	pub const TABLE: u32 = 1 << 5;
	pub const DATETIME: u32 = 1 << 6;
	pub const DURATION: u32 = 1 << 7;
	pub const UUID: u32 = 1 << 8;
	pub const BYTES: u32 = 1 << 9;
	pub const OBJECT: u32 = 1 << 10;
	pub const ARRAY: u32 = 1 << 11;
	pub const SET: u32 = 1 << 12;
	pub const RECORD: u32 = 1 << 13;
	pub const GEOMETRY: u32 = 1 << 14;
	pub const REGEX: u32 = 1 << 15;
	pub const RANGE: u32 = 1 << 16;
	pub const CLOSURE: u32 = 1 << 17;
	pub const FILE: u32 = 1 << 18;
	pub const DATE: u32 = 1 << 19;
	pub const TIME: u32 = 1 << 20;
	pub const TIMESTAMP: u32 = 1 << 21;
	pub const INTERVAL: u32 = 1 << 22;
	pub const ALL: u32 = u32::MAX;
}

impl Kind {
	/// Whether every value that conforms to `self` — one that coercion to
	/// `self` returns unchanged — also conforms to `new`, unchanged. A field
	/// retyped from `self` to `new` then keeps every stored value valid, so the
	/// retype needs no migration and keeps the field's conformance.
	///
	/// Conservative: `true` only with proof, so a `false` may be a widening
	/// this test does not recognise. A false positive would silently break a
	/// guarantee, where a false negative only asks for a harmless migration.
	///
	/// A union coerces to its first arm that accepts the value, so `new` being
	/// a union widens `self` only when an arm that `self` widens to comes
	/// before every arm that could capture a value of `self` and change it:
	/// `int` widens to `int | float` but not to `float | int`.
	pub fn widens_to(&self, new: &Kind) -> bool {
		if self == new || matches!(new, Kind::Any) {
			return true;
		}
		match (self, new) {
			// A value conforming to a union conforms, unchanged, to one of its
			// arms.
			(Kind::Either(old), _) => !old.is_empty() && old.iter().all(|k| k.widens_to(new)),
			(_, Kind::Either(arms)) => {
				let domain = self.coerce_domain();
				for arm in arms {
					if self.widens_to(arm) {
						return true;
					}
					// `NONE` and `NULL` coerce to themselves alone and unchanged, so an arm of
					// either changes no value the later arms would take.
					if matches!(arm, Kind::None | Kind::Null) {
						continue;
					}
					if domain & arm.coerce_domain() != 0 && !self.excludes_literal(arm) {
						return false;
					}
				}
				false
			}
			(
				Kind::Int
				| Kind::Float
				| Kind::Decimal
				| Kind::BigDecimal
				| Kind::I8
				| Kind::U8
				| Kind::I16
				| Kind::U16
				| Kind::I32
				| Kind::U32
				| Kind::U64
				| Kind::F16
				| Kind::F32,
				Kind::Number,
			) => true,
			(Kind::Array(old, old_len), Kind::Array(new, new_len))
			| (Kind::Set(old, old_len), Kind::Set(new, new_len)) => {
				(new_len.is_none() || old_len == new_len) && old.widens_to(new)
			}
			(Kind::Record(old), Kind::Record(new)) | (Kind::Table(old), Kind::Table(new)) => {
				new.is_empty() || (!old.is_empty() && old.iter().all(|t| new.contains(t)))
			}
			(Kind::Geometry(old), Kind::Geometry(new)) => {
				new.is_empty() || (!old.is_empty() && old.iter().all(|g| new.contains(g)))
			}
			(Kind::File(old), Kind::File(new)) => {
				new.is_empty() || (!old.is_empty() && old.iter().all(|b| new.contains(b)))
			}
			// An enum is a string, and an inline enum widens to one whose labels include its own.
			(Kind::Enum(_), Kind::String) => true,
			(Kind::Enum(old), Kind::Enum(new)) => old.iter().all(|l| new.contains(l)),
			_ => false,
		}
	}

	/// Whether `self` and `other` are literal kinds no value conforms to both:
	/// two string literals, or two integer literals, of different values. Any
	/// other pair is not claimed disjoint, since a literal may coerce a value
	/// of another kind.
	fn excludes_literal(&self, other: &Kind) -> bool {
		match (self, other) {
			(Kind::Literal(KindLiteral::String(a)), Kind::Literal(KindLiteral::String(b))) => {
				a != b
			}
			(Kind::Literal(KindLiteral::Integer(a)), Kind::Literal(KindLiteral::Integer(b))) => {
				a != b
			}
			_ => false,
		}
	}

	/// The value variants coercion to this kind accepts; see
	/// [`coerce_domain`].
	fn coerce_domain(&self) -> u32 {
		use coerce_domain as d;
		match self {
			Kind::Any | Kind::Literal(_) => d::ALL,
			Kind::None => d::NONE,
			Kind::Null => d::NULL,
			Kind::Bool => d::BOOL,
			Kind::Int
			| Kind::Float
			| Kind::Decimal
			| Kind::BigDecimal
			| Kind::Number
			| Kind::I8
			| Kind::U8
			| Kind::I16
			| Kind::U16
			| Kind::I32
			| Kind::U32
			| Kind::U64
			| Kind::F16
			| Kind::F32 => d::NUMBER,
			Kind::String => d::STRING,
			// A string coerces to a table name.
			Kind::Table(_) => d::STRING | d::TABLE,
			// A datetime, a date and a timestamp each coerce from the other two.
			Kind::Datetime | Kind::Date | Kind::Timestamp => d::DATETIME | d::DATE | d::TIMESTAMP,
			Kind::Time => d::TIME,
			Kind::Interval => d::INTERVAL,
			Kind::Duration => d::DURATION,
			Kind::Uuid => d::UUID,
			Kind::Bytes => d::BYTES,
			Kind::Object => d::OBJECT,
			// An array coerces to a set.
			Kind::Array(..) | Kind::Set(..) => d::ARRAY | d::SET,
			Kind::Record(_) => d::RECORD,
			// A point coerces from an array of coordinates, and any geometry
			// from the GeoJSON object describing it.
			Kind::Geometry(_) => d::GEOMETRY | d::ARRAY | d::OBJECT,
			Kind::Regex => d::REGEX,
			Kind::Range => d::RANGE,
			Kind::Function(..) => d::CLOSURE,
			Kind::File(_) => d::FILE,
			// A string coerces into an enum; membership is judged where the labels are known.
			Kind::Enum(_) => d::STRING,
			// A named type admits what its body admits, which only the catalog knows, so it is
			// taken to admit every value.
			Kind::Named(_) => d::ALL,
			Kind::Either(kinds) => kinds.iter().fold(0, |acc, k| acc | k.coerce_domain()),
		}
	}

	/// Whether this kind is, or contains, a named type.
	pub fn has_named_type(&self) -> bool {
		let mut found = false;
		self.each_named_type(&mut |_| found = true);
		found
	}

	/// Whether this kind is, or contains, the named type `name`.
	pub fn references_type(&self, name: &str) -> bool {
		let mut found = false;
		self.each_named_type(&mut |n| found |= n == name);
		found
	}

	/// Calls `f` with the name of each named type this kind is, or contains, at any depth of the
	/// kind itself. The bodies of the named types are not read.
	pub fn each_named_type(&self, f: &mut dyn FnMut(&str)) {
		match self {
			Kind::Named(name) => f(name),
			Kind::Either(kinds) | Kind::Literal(KindLiteral::Array(kinds)) => {
				kinds.iter().for_each(|k| k.each_named_type(f))
			}
			Kind::Array(inner, _) | Kind::Set(inner, _) => inner.each_named_type(f),
			Kind::Literal(KindLiteral::Object(fields)) => {
				fields.values().for_each(|k| k.each_named_type(f))
			}
			Kind::Function(args, ret) => {
				args.iter().flatten().for_each(|k| k.each_named_type(f));
				if let Some(ret) = ret {
					ret.each_named_type(f);
				}
			}
			Kind::Any
			| Kind::None
			| Kind::Null
			| Kind::Bool
			| Kind::Bytes
			| Kind::Datetime
			| Kind::Date
			| Kind::Time
			| Kind::Timestamp
			| Kind::Interval
			| Kind::Decimal
			| Kind::BigDecimal
			| Kind::Duration
			| Kind::Float
			| Kind::Int
			| Kind::I8
			| Kind::U8
			| Kind::I16
			| Kind::U16
			| Kind::I32
			| Kind::U32
			| Kind::U64
			| Kind::F16
			| Kind::F32
			| Kind::Number
			| Kind::Object
			| Kind::String
			| Kind::Uuid
			| Kind::Regex
			| Kind::Table(_)
			| Kind::Record(_)
			| Kind::Geometry(_)
			| Kind::Range
			| Kind::File(_)
			| Kind::Enum(_)
			| Kind::Literal(
				KindLiteral::String(_)
				| KindLiteral::Integer(_)
				| KindLiteral::Float(_)
				| KindLiteral::Decimal(_)
				| KindLiteral::Duration(_)
				| KindLiteral::Bool(_),
			) => {}
		}
	}

	/// This kind with each named type it is, or contains, at any depth of the kind itself,
	/// replaced by what `f` gives for its name; a named type `f` gives `None` for is kept.
	pub fn replace_named_types(&self, f: &mut dyn FnMut(&str) -> Option<Kind>) -> Kind {
		match self {
			Kind::Named(name) => f(name).unwrap_or_else(|| self.clone()),
			Kind::Either(kinds) => {
				Kind::Either(kinds.iter().map(|k| k.replace_named_types(f)).collect())
			}
			Kind::Literal(KindLiteral::Array(kinds)) => Kind::Literal(KindLiteral::Array(
				kinds.iter().map(|k| k.replace_named_types(f)).collect(),
			)),
			Kind::Array(inner, len) => Kind::Array(Box::new(inner.replace_named_types(f)), *len),
			Kind::Set(inner, len) => Kind::Set(Box::new(inner.replace_named_types(f)), *len),
			Kind::Literal(KindLiteral::Object(fields)) => Kind::Literal(KindLiteral::Object(
				fields.iter().map(|(key, k)| (key.clone(), k.replace_named_types(f))).collect(),
			)),
			Kind::Function(args, ret) => Kind::Function(
				args.as_ref().map(|args| args.iter().map(|k| k.replace_named_types(f)).collect()),
				ret.as_ref().map(|ret| Box::new(ret.replace_named_types(f))),
			),
			Kind::Any
			| Kind::None
			| Kind::Null
			| Kind::Bool
			| Kind::Bytes
			| Kind::Datetime
			| Kind::Date
			| Kind::Time
			| Kind::Timestamp
			| Kind::Interval
			| Kind::Decimal
			| Kind::BigDecimal
			| Kind::Duration
			| Kind::Float
			| Kind::Int
			| Kind::I8
			| Kind::U8
			| Kind::I16
			| Kind::U16
			| Kind::I32
			| Kind::U32
			| Kind::U64
			| Kind::F16
			| Kind::F32
			| Kind::Number
			| Kind::Object
			| Kind::String
			| Kind::Uuid
			| Kind::Regex
			| Kind::Table(_)
			| Kind::Record(_)
			| Kind::Geometry(_)
			| Kind::Range
			| Kind::File(_)
			| Kind::Enum(_)
			| Kind::Literal(
				KindLiteral::String(_)
				| KindLiteral::Integer(_)
				| KindLiteral::Float(_)
				| KindLiteral::Decimal(_)
				| KindLiteral::Duration(_)
				| KindLiteral::Bool(_),
			) => self.clone(),
		}
	}

	/// Returns the kind of a type.
	pub fn of<T: HasKind>() -> Kind {
		T::kind()
	}

	/// All sized numeric kinds (`i8`, `u8`, `i16`, `u16`, `i32`, `u32`, `u64`, `f16`, `f32`).
	pub const SIZED: [Kind; 9] = [
		Kind::I8,
		Kind::U8,
		Kind::I16,
		Kind::U16,
		Kind::I32,
		Kind::U32,
		Kind::U64,
		Kind::F16,
		Kind::F32,
	];

	/// All numeric kinds (`int`, `float`, `decimal`, `number`, and all [`Self::SIZED`] kinds).
	pub const NUMERIC: [Kind; 14] = [
		Kind::Int,
		Kind::Float,
		Kind::Decimal,
		Kind::BigDecimal,
		Kind::Number,
		Kind::I8,
		Kind::U8,
		Kind::I16,
		Kind::U16,
		Kind::I32,
		Kind::U32,
		Kind::U64,
		Kind::F16,
		Kind::F32,
	];

	/// Returns `true` if this kind is one of the sized numeric kinds (`i8`..`f32`).
	pub const fn is_sized_number(&self) -> bool {
		matches!(
			self,
			Kind::I8
				| Kind::U8 | Kind::I16
				| Kind::U16 | Kind::I32
				| Kind::U32 | Kind::U64
				| Kind::F16 | Kind::F32
		)
	}

	/// Returns `true` if this kind admits only numbers (`int`, `float`, `decimal`, `number`, or a
	/// sized numeric kind).
	pub const fn is_numeric(&self) -> bool {
		matches!(self, Kind::Int | Kind::Float | Kind::Decimal | Kind::BigDecimal | Kind::Number)
			|| self.is_sized_number()
	}

	/// Returns true if this type is an `any`
	pub fn is_any(&self) -> bool {
		matches!(self, Kind::Any)
	}

	/// Returns true if this type is a record
	pub fn is_record(&self) -> bool {
		matches!(self, Kind::Record(_))
	}

	/// Returns `true` if a `DEFINE FIELD ... REFERENCE` declared with this kind
	/// could hold a record id belonging to `table`.
	///
	/// A reference key is only ever written (by `process_reference_clause`)
	/// when a reference field holds a record id, and the field's declared kind
	/// constrains which tables those ids may belong to via TYPE coercion. So
	/// this answers, from the schema alone, whether records in `table` can have
	/// incoming reference keys — used by the DELETE purge path to skip the
	/// per-record reference scan when they provably cannot.
	///
	/// The matched shapes mirror exactly the kinds accepted for reference
	/// fields by [`crate::expr::statements::DefineFieldStatement::validate_reference_options`]:
	/// `record<..>`, `array`/`set` of those, `option<..>` (an `Either`
	/// containing `None`), and literal arrays of those. An untyped `record`
	/// (`Kind::Record` with an empty table list) can hold a record of *any*
	/// table, so it always matches. `Kind::Any` is rejected for reference
	/// fields at definition time but is treated as a match here defensively —
	/// erring towards running the scan is always safe.
	pub fn reference_can_target(&self, table: &TableName) -> bool {
		match self {
			// What a named type holds is the catalog's to say, which this does not read.
			Kind::Named(_) => true,
			Kind::Record(tables) => tables.is_empty() || tables.iter().any(|t| t == table),
			Kind::Array(inner, _) | Kind::Set(inner, _) => inner.reference_can_target(table),
			Kind::Either(kinds) => kinds.iter().any(|k| k.reference_can_target(table)),
			Kind::Literal(KindLiteral::Array(kinds)) => {
				kinds.iter().any(|k| k.reference_can_target(table))
			}
			Kind::Any => true,
			Kind::None
			| Kind::Null
			| Kind::Bool
			| Kind::Bytes
			| Kind::Datetime
			| Kind::Date
			| Kind::Time
			| Kind::Timestamp
			| Kind::Interval
			| Kind::Decimal
			| Kind::BigDecimal
			| Kind::Duration
			| Kind::Float
			| Kind::Int
			| Kind::Number
			| Kind::Object
			| Kind::String
			| Kind::Uuid
			| Kind::Regex
			| Kind::Table(_)
			| Kind::Geometry(_)
			| Kind::Function(_, _)
			| Kind::Range
			| Kind::File(_)
			| Kind::Enum(_)
			| Kind::I8
			| Kind::U8
			| Kind::I16
			| Kind::U16
			| Kind::I32
			| Kind::U32
			| Kind::U64
			| Kind::F16
			| Kind::F32
			| Kind::Literal(
				KindLiteral::String(_)
				| KindLiteral::Integer(_)
				| KindLiteral::Float(_)
				| KindLiteral::Decimal(_)
				| KindLiteral::Duration(_)
				| KindLiteral::Object(_)
				| KindLiteral::Bool(_),
			) => false,
		}
	}

	/// Collects the concrete record tables a `REFERENCE` field of this kind can
	/// target into `out`, returning `true` if the kind can target *any* table
	/// (an untyped `record` / `record<>`, or — defensively — `any`), in which
	/// case `out` is not exhaustive and every table must be treated as
	/// potentially referenced.
	///
	/// This is the enumerating dual of [`Self::reference_can_target`]: it lets a
	/// caller build a database-wide set of referenceable tables once (e.g. the
	/// memoized DELETE purge gate) rather than re-deciding per table. The
	/// matched shapes are identical to that method's.
	pub fn collect_reference_target_tables(&self, out: &mut HashSet<TableName>) -> bool {
		match self {
			// What a named type holds is the catalog's to say, which this does not read.
			Kind::Named(_) => true,
			Kind::Record(tables) => {
				if tables.is_empty() {
					return true;
				}
				out.extend(tables.iter().cloned());
				false
			}
			Kind::Array(inner, _) | Kind::Set(inner, _) => {
				inner.collect_reference_target_tables(out)
			}
			Kind::Either(kinds) => {
				let mut unbounded = false;
				for k in kinds {
					unbounded |= k.collect_reference_target_tables(out);
				}
				unbounded
			}
			Kind::Literal(KindLiteral::Array(kinds)) => {
				let mut unbounded = false;
				for k in kinds {
					unbounded |= k.collect_reference_target_tables(out);
				}
				unbounded
			}
			Kind::Any => true,
			Kind::None
			| Kind::Null
			| Kind::Bool
			| Kind::Bytes
			| Kind::Datetime
			| Kind::Date
			| Kind::Time
			| Kind::Timestamp
			| Kind::Interval
			| Kind::Decimal
			| Kind::BigDecimal
			| Kind::Duration
			| Kind::Float
			| Kind::Int
			| Kind::Number
			| Kind::Object
			| Kind::String
			| Kind::Uuid
			| Kind::Regex
			| Kind::Table(_)
			| Kind::Geometry(_)
			| Kind::Function(_, _)
			| Kind::Range
			| Kind::File(_)
			| Kind::Enum(_)
			| Kind::I8
			| Kind::U8
			| Kind::I16
			| Kind::U16
			| Kind::I32
			| Kind::U32
			| Kind::U64
			| Kind::F16
			| Kind::F32
			| Kind::Literal(
				KindLiteral::String(_)
				| KindLiteral::Integer(_)
				| KindLiteral::Float(_)
				| KindLiteral::Decimal(_)
				| KindLiteral::Duration(_)
				| KindLiteral::Object(_)
				| KindLiteral::Bool(_),
			) => false,
		}
	}

	/// Returns true if this type is optional
	pub fn can_be_none(&self) -> bool {
		match self {
			// What a named type holds is the catalog's to say, which this does not read.
			Kind::Named(_) => true,
			Kind::None | Kind::Any => true,
			Kind::Either(x) => x.iter().any(|x| x.can_be_none()),
			Kind::Null
			| Kind::Bool
			| Kind::Bytes
			| Kind::Datetime
			| Kind::Date
			| Kind::Time
			| Kind::Timestamp
			| Kind::Interval
			| Kind::Decimal
			| Kind::BigDecimal
			| Kind::Duration
			| Kind::Float
			| Kind::Int
			| Kind::Number
			| Kind::Object
			| Kind::String
			| Kind::Uuid
			| Kind::Regex
			| Kind::Table(_)
			| Kind::Record(_)
			| Kind::Geometry(_)
			| Kind::Set(_, _)
			| Kind::Array(_, _)
			| Kind::Function(_, _)
			| Kind::Range
			| Kind::Literal(_)
			| Kind::File(_)
			| Kind::Enum(_)
			| Kind::I8
			| Kind::U8
			| Kind::I16
			| Kind::U16
			| Kind::I32
			| Kind::U32
			| Kind::U64
			| Kind::F16
			| Kind::F32 => false,
		}
	}

	/// Returns true if this type is a literal, or contains a literal
	pub fn contains_literal(&self) -> bool {
		match self {
			// What a named type holds is the catalog's to say, which this does not read.
			Kind::Named(_) => false,
			Kind::Literal(_) => true,
			Kind::Either(x) => x.iter().any(|x| x.contains_literal()),
			Kind::Any
			| Kind::None
			| Kind::Null
			| Kind::Bool
			| Kind::Bytes
			| Kind::Datetime
			| Kind::Date
			| Kind::Time
			| Kind::Timestamp
			| Kind::Interval
			| Kind::Decimal
			| Kind::BigDecimal
			| Kind::Duration
			| Kind::Float
			| Kind::Int
			| Kind::Number
			| Kind::Object
			| Kind::String
			| Kind::Uuid
			| Kind::Regex
			| Kind::Table(_)
			| Kind::Record(_)
			| Kind::Geometry(_)
			| Kind::Set(_, _)
			| Kind::Array(_, _)
			| Kind::Function(_, _)
			| Kind::Range
			| Kind::File(_)
			| Kind::Enum(_)
			| Kind::I8
			| Kind::U8
			| Kind::I16
			| Kind::U16
			| Kind::I32
			| Kind::U32
			| Kind::U64
			| Kind::F16
			| Kind::F32 => false,
		}
	}

	/// Returns true if this type permits access to named sub-fields.
	///
	/// This covers `object`, `any`, literal object types (e.g. `{ key: string }`),
	/// and union types where every non-none variant permits sub-field access.
	pub fn allows_sub_fields(&self) -> bool {
		match self {
			// What a named type holds is the catalog's to say, which this does not read.
			Kind::Named(_) => true,
			Kind::Any | Kind::Object | Kind::Array(..) | Kind::Set(..) => true,
			Kind::Literal(KindLiteral::Object(_) | KindLiteral::Array(_)) => true,
			Kind::Either(kinds) => {
				kinds.iter().all(|k| matches!(k, Kind::None | Kind::Null) || k.allows_sub_fields())
			}
			Kind::None
			| Kind::Null
			| Kind::Bool
			| Kind::Bytes
			| Kind::Datetime
			| Kind::Date
			| Kind::Time
			| Kind::Timestamp
			| Kind::Interval
			| Kind::Decimal
			| Kind::BigDecimal
			| Kind::Duration
			| Kind::Float
			| Kind::Int
			| Kind::Number
			| Kind::String
			| Kind::Uuid
			| Kind::Regex
			| Kind::Table(_)
			| Kind::Record(_)
			| Kind::Geometry(_)
			| Kind::Function(_, _)
			| Kind::Range
			| Kind::File(_)
			| Kind::Enum(_)
			| Kind::I8
			| Kind::U8
			| Kind::I16
			| Kind::U16
			| Kind::I32
			| Kind::U32
			| Kind::U64
			| Kind::F16
			| Kind::F32
			| Kind::Literal(
				KindLiteral::String(_)
				| KindLiteral::Integer(_)
				| KindLiteral::Float(_)
				| KindLiteral::Decimal(_)
				| KindLiteral::Duration(_)
				| KindLiteral::Bool(_),
			) => false,
		}
	}

	// Return the kind of the contained value.
	//
	// For example: for `array<number>` or `set<number>` this returns `number`.
	// For `array<number> | set<float>` this returns `number | float`.
	pub fn inner_kind(&self) -> Option<Kind> {
		match self {
			// What a named type holds is the catalog's to say, which this does not read.
			Kind::Named(_) => None,
			Kind::Any
			| Kind::None
			| Kind::Null
			| Kind::Bool
			| Kind::Bytes
			| Kind::Datetime
			| Kind::Date
			| Kind::Time
			| Kind::Timestamp
			| Kind::Interval
			| Kind::Decimal
			| Kind::BigDecimal
			| Kind::Duration
			| Kind::Float
			| Kind::Int
			| Kind::Number
			| Kind::Object
			| Kind::String
			| Kind::Uuid
			| Kind::Regex
			| Kind::Table(_)
			| Kind::Record(_)
			| Kind::Geometry(_)
			| Kind::Function(_, _)
			| Kind::Range
			| Kind::Literal(_)
			| Kind::File(_)
			| Kind::Enum(_)
			| Kind::I8
			| Kind::U8
			| Kind::I16
			| Kind::U16
			| Kind::I32
			| Kind::U32
			| Kind::U64
			| Kind::F16
			| Kind::F32 => None,
			Kind::Array(x, _) | Kind::Set(x, _) => Some(x.as_ref().clone()),
			Kind::Either(x) => {
				// a either shouldn't be able to contain a either itself so recursing here
				// should be fine.
				let kinds: Vec<Kind> = x.iter().filter_map(Self::inner_kind).collect();
				if kinds.is_empty() {
					None
				} else {
					Some(Kind::Either(kinds))
				}
			}
		}
	}

	pub fn allows_nested_kind(&self, path: &[Part], kind: &Kind) -> bool {
		// ANY type won't cause a mismatch
		if self.is_any() || kind.is_any() {
			return true;
		}

		if !path.is_empty() {
			match self {
				// What a named type holds is the catalog's to say, which this does not read.
				Kind::Named(_) => return true,
				Kind::Object => return matches!(path.first(), Some(Part::Field(_) | Part::All)),
				Kind::Either(kinds) => {
					return kinds
						.iter()
						.all(|k| matches!(k, Kind::None) || k.allows_nested_kind(path, kind));
				}
				Kind::Array(inner, len) | Kind::Set(inner, len) => {
					return match path.first() {
						Some(Part::All) => inner.allows_nested_kind(&path[1..], kind),
						Some(Part::Value(Expr::Literal(Literal::Integer(i)))) => {
							if let Some(len) = len
								&& *i >= *len as i64
							{
								return false;
							}

							inner.allows_nested_kind(&path[1..], kind)
						}
						_ => false,
					};
				}
				Kind::Any
				| Kind::None
				| Kind::Null
				| Kind::Bool
				| Kind::Bytes
				| Kind::Datetime
				| Kind::Date
				| Kind::Time
				| Kind::Timestamp
				| Kind::Interval
				| Kind::Decimal
				| Kind::BigDecimal
				| Kind::Duration
				| Kind::Float
				| Kind::Int
				| Kind::Number
				| Kind::String
				| Kind::Uuid
				| Kind::Regex
				| Kind::Table(_)
				| Kind::Record(_)
				| Kind::Geometry(_)
				| Kind::Function(_, _)
				| Kind::Range
				| Kind::Literal(_)
				| Kind::File(_)
				| Kind::Enum(_)
				| Kind::I8
				| Kind::U8
				| Kind::I16
				| Kind::U16
				| Kind::I32
				| Kind::U32
				| Kind::U64
				| Kind::F16
				| Kind::F32 => (),
			}
		}

		// Check if the two kinds match when we reach the end of the path
		if path.is_empty() && self == kind {
			return true;
		}

		match self {
			// What a named type holds is the catalog's to say, which this does not read.
			Kind::Named(_) => false,
			// Check if the literal matches the kind
			Kind::Literal(lit) => lit.allows_nested_kind(path, kind),
			// Check if any of the kinds in the either match the kind
			Kind::Either(kinds) => {
				kinds.iter().all(|k| matches!(k, Kind::None) || k.allows_nested_kind(path, kind))
			}
			Kind::Any
			| Kind::None
			| Kind::Null
			| Kind::Bool
			| Kind::Bytes
			| Kind::Datetime
			| Kind::Date
			| Kind::Time
			| Kind::Timestamp
			| Kind::Interval
			| Kind::Decimal
			| Kind::BigDecimal
			| Kind::Duration
			| Kind::Float
			| Kind::Int
			| Kind::Number
			| Kind::Object
			| Kind::String
			| Kind::Uuid
			| Kind::Regex
			| Kind::Table(_)
			| Kind::Record(_)
			| Kind::Geometry(_)
			| Kind::Set(_, _)
			| Kind::Array(_, _)
			| Kind::Function(_, _)
			| Kind::Range
			| Kind::File(_)
			| Kind::Enum(_)
			| Kind::I8
			| Kind::U8
			| Kind::I16
			| Kind::U16
			| Kind::I32
			| Kind::U32
			| Kind::U64
			| Kind::F16
			| Kind::F32 => false,
		}
	}

	pub fn flatten(self) -> Vec<Kind> {
		if let Kind::Either(x) = self {
			x.into_iter().flat_map(|k| k.flatten()).collect()
		} else {
			vec![self]
		}
	}

	pub fn either(kinds: Vec<Kind>) -> Kind {
		let mut seen = HashSet::new();
		let mut kinds = kinds
			.into_iter()
			.flat_map(|k| k.flatten())
			.filter(|k| seen.insert(k.clone()))
			.collect::<Vec<_>>();
		match kinds.len() {
			0 => Kind::None,
			1 => kinds.remove(0),
			_ => Kind::Either(kinds),
		}
	}

	pub fn option(kind: Kind) -> Kind {
		Kind::either(vec![Kind::None, kind])
	}
}

/// Trait for retrieving the `kind` equivalent of a rust type.
///
/// Returns the most general kind for a type.
/// For example Number could be either number or float or int or decimal but the
/// most general is number.
///
/// This trait is only implemented for types which can only be retrieve from
pub trait HasKind {
	fn kind() -> Kind;
}

impl<T: HasKind> HasKind for Option<T> {
	fn kind() -> Kind {
		Kind::option(T::kind())
	}
}

impl<T: HasKind> HasKind for Vec<T> {
	fn kind() -> Kind {
		let kind = T::kind();
		Kind::Array(Box::new(kind), None)
	}
}

impl HasKind for Array {
	fn kind() -> Kind {
		Kind::Array(Box::new(Kind::Any), None)
	}
}

impl HasKind for Set {
	fn kind() -> Kind {
		Kind::Set(Box::new(Kind::Any), None)
	}
}

impl<T: HasKind, const SIZE: usize> HasKind for [T; SIZE] {
	fn kind() -> Kind {
		let kind = T::kind();
		Kind::Array(Box::new(kind), Some(SIZE as u64))
	}
}

impl HasKind for RecordId {
	fn kind() -> Kind {
		Kind::Record(Vec::new())
	}
}

impl HasKind for Geometry {
	fn kind() -> Kind {
		Kind::Geometry(Vec::new())
	}
}

impl HasKind for Closure {
	fn kind() -> Kind {
		// The inner values of function are currently completely unused.
		Kind::Function(None, None)
	}
}

impl HasKind for Regex {
	fn kind() -> Kind {
		Kind::Regex
	}
}

impl HasKind for File {
	fn kind() -> Kind {
		Kind::File(Vec::new())
	}
}

impl HasKind for TableName {
	fn kind() -> Kind {
		Kind::Table(Vec::new())
	}
}

macro_rules! impl_basic_has_kind{
	($($name:ident => $kind:ident),*$(,)?) => {
		$(
			impl HasKind for $name{
				fn kind() -> Kind{
					Kind::$kind
				}
			}
		)*
	}
}

impl_basic_has_kind! {
	bool => Bool,

	i8 => I8,
	u8 => U8,
	i16 => I16,
	u16 => U16,
	i32 => I32,
	u32 => U32,
	i64 => Int,
	u64 => U64,
	f32 => F32,
	f64 => Float,
	Decimal => Decimal,

	String => String,
	Bytes => Bytes,
	Number => Number,
	Datetime => Datetime,
	Date => Date,
	Time => Time,
	Timestamp => Timestamp,
	Interval => Interval,
	Duration => Duration,
	Uuid => Uuid,
	Range => Range,
}

impl HasKind for half::f16 {
	fn kind() -> Kind {
		Kind::F16
	}
}

impl HasKind for crate::val::Object {
	fn kind() -> Kind {
		Kind::Object
	}
}

macro_rules! impl_geometry_has_kind{
	($($name:ty => $kind:expr),*$(,)?) => {
		$(
			impl HasKind for $name{
				fn kind() -> Kind{
					Kind::Geometry(vec![$kind])
				}
			}
		)*
	}
}
impl_geometry_has_kind! {
	Point<f64> => GeometryKind::Point,
	LineString<f64> => GeometryKind::Line,
	MultiPoint<f64> => GeometryKind::MultiPoint,
	Polygon<f64> => GeometryKind::Polygon,
	MultiLineString<f64> => GeometryKind::MultiLine,
	MultiPolygon<f64> => GeometryKind::MultiPolygon,
}

impl From<&Kind> for Box<Kind> {
	#[inline]
	fn from(v: &Kind) -> Self {
		Box::new(v.clone())
	}
}

impl ToSql for Kind {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Kind::Any => f.push_str("any"),
			Kind::None => f.push_str("none"),
			Kind::Null => f.push_str("null"),
			Kind::Bool => f.push_str("bool"),
			Kind::Bytes => f.push_str("bytes"),
			Kind::Datetime => f.push_str("datetime"),
			Kind::Date => f.push_str("date"),
			Kind::Time => f.push_str("time"),
			Kind::Timestamp => f.push_str("timestamp"),
			Kind::Interval => f.push_str("interval"),
			Kind::Decimal => f.push_str("decimal"),
			Kind::BigDecimal => f.push_str("bigdecimal"),
			Kind::Duration => f.push_str("duration"),
			Kind::Float => f.push_str("float"),
			Kind::Int => f.push_str("int"),
			Kind::Number => f.push_str("number"),
			Kind::Object => f.push_str("object"),
			Kind::String => f.push_str("string"),
			Kind::Uuid => f.push_str("uuid"),
			Kind::Regex => f.push_str("regex"),
			Kind::Function(_, _) => f.push_str("function"),
			Kind::Table(k) => {
				if k.is_empty() {
					f.push_str("table");
				} else {
					write_sql!(
						f,
						fmt,
						"table<{}>",
						Fmt::verbar_separated(
							k.iter().map(|x| common::fmt::EscapeIdent(x.as_str()))
						)
					);
				}
			}
			Kind::Record(k) => {
				if k.is_empty() {
					f.push_str("record");
				} else {
					write_sql!(
						f,
						fmt,
						"record<{}>",
						Fmt::verbar_separated(
							k.iter().map(|x| common::fmt::EscapeIdent(x.as_str()))
						)
					);
				}
			}
			Kind::Geometry(k) => {
				if k.is_empty() {
					f.push_str("geometry");
				} else {
					write_sql!(f, fmt, "geometry<{}>", Fmt::verbar_separated(k));
				}
			}
			Kind::Set(k, l) => match (k, l) {
				(k, None) if matches!(**k, Kind::Any) => f.push_str("set"),
				(k, None) => write_sql!(f, fmt, "set<{k}>"),
				(k, Some(l)) => write_sql!(f, fmt, "set<{k}, {l}>"),
			},
			Kind::Array(k, l) => match (k, l) {
				(k, None) if matches!(**k, Kind::Any) => f.push_str("array"),
				(k, None) => write_sql!(f, fmt, "array<{k}>"),
				(k, Some(l)) => write_sql!(f, fmt, "array<{k}, {l}>"),
			},
			Kind::Either(k) => write_sql!(f, fmt, "{}", Fmt::verbar_separated(k)),
			Kind::Range => f.push_str("range"),
			Kind::Literal(l) => l.fmt_sql(f, fmt),
			Kind::Enum(labels) => {
				write_sql!(
					f,
					fmt,
					"enum<{}>",
					Fmt::comma_separated(labels.iter().map(|l| QuoteStr(l)))
				)
			}
			Kind::Named(name) => write_sql!(f, fmt, "{}", EscapeKwFreeIdent(name)),
			Kind::File(k) => {
				if k.is_empty() {
					f.push_str("file");
				} else {
					write_sql!(
						f,
						fmt,
						"file<{}>",
						Fmt::verbar_separated(k.iter().map(|x| EscapeKwFreeIdent(x)))
					);
				}
			}
			Kind::I8 => f.push_str("i8"),
			Kind::U8 => f.push_str("u8"),
			Kind::I16 => f.push_str("i16"),
			Kind::U16 => f.push_str("u16"),
			Kind::I32 => f.push_str("i32"),
			Kind::U32 => f.push_str("u32"),
			Kind::U64 => f.push_str("u64"),
			Kind::F16 => f.push_str("f16"),
			Kind::F32 => f.push_str("f32"),
		}
	}
}

impl Display for Kind {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "{}", self.to_sql())
	}
}

impl InfoStructure for Kind {
	fn structure(self) -> Value {
		self.to_string().into()
	}
}

impl From<crate::types::PublicKind> for Kind {
	fn from(v: crate::types::PublicKind) -> Self {
		match v {
			crate::types::PublicKind::Any => Kind::Any,
			crate::types::PublicKind::None => Kind::None,
			crate::types::PublicKind::Null => Kind::Null,
			crate::types::PublicKind::Bool => Kind::Bool,
			crate::types::PublicKind::Bytes => Kind::Bytes,
			crate::types::PublicKind::Datetime => Kind::Datetime,
			crate::types::PublicKind::Date => Kind::Date,
			crate::types::PublicKind::Time => Kind::Time,
			crate::types::PublicKind::Timestamp => Kind::Timestamp,
			crate::types::PublicKind::Interval => Kind::Interval,
			crate::types::PublicKind::Decimal => Kind::Decimal,
			crate::types::PublicKind::BigDecimal => Kind::BigDecimal,
			crate::types::PublicKind::Duration => Kind::Duration,
			crate::types::PublicKind::Float => Kind::Float,
			crate::types::PublicKind::Int => Kind::Int,
			crate::types::PublicKind::Number => Kind::Number,
			crate::types::PublicKind::Object => Kind::Object,
			crate::types::PublicKind::String => Kind::String,
			crate::types::PublicKind::Uuid => Kind::Uuid,
			crate::types::PublicKind::Regex => Kind::Regex,
			crate::types::PublicKind::Range => Kind::Range,
			crate::types::PublicKind::Table(table) => {
				Kind::Table(table.into_iter().map(IntoTableName::into_table_name).collect())
			}
			crate::types::PublicKind::Record(tables) => {
				Kind::Record(tables.into_iter().map(IntoTableName::into_table_name).collect())
			}
			crate::types::PublicKind::Geometry(kinds) => {
				Kind::Geometry(kinds.into_iter().map(Into::into).collect())
			}
			crate::types::PublicKind::Either(kinds) => {
				Kind::Either(kinds.into_iter().map(Kind::from).collect())
			}
			crate::types::PublicKind::Set(kind, size) => {
				Kind::Set(Box::new(Kind::from(*kind)), size)
			}
			crate::types::PublicKind::Array(kind, size) => {
				Kind::Array(Box::new(Kind::from(*kind)), size)
			}
			crate::types::PublicKind::Function(args, ret) => Kind::Function(
				args.map(|a| a.into_iter().map(Kind::from).collect()),
				ret.map(|r| Box::new(Kind::from(*r))),
			),
			crate::types::PublicKind::File(bucket) => Kind::File(bucket),
			crate::types::PublicKind::Enum(labels) => Kind::Enum(labels),
			crate::types::PublicKind::Named(name) => Kind::Named(name),
			crate::types::PublicKind::Literal(lit) => Kind::Literal(lit.into()),
			crate::types::PublicKind::I8 => Kind::I8,
			crate::types::PublicKind::U8 => Kind::U8,
			crate::types::PublicKind::I16 => Kind::I16,
			crate::types::PublicKind::U16 => Kind::U16,
			crate::types::PublicKind::I32 => Kind::I32,
			crate::types::PublicKind::U32 => Kind::U32,
			crate::types::PublicKind::U64 => Kind::U64,
			crate::types::PublicKind::F16 => Kind::F16,
			crate::types::PublicKind::F32 => Kind::F32,
		}
	}
}

impl From<Kind> for crate::types::PublicKind {
	fn from(v: Kind) -> Self {
		match v {
			Kind::Any => crate::types::PublicKind::Any,
			Kind::None => crate::types::PublicKind::None,
			Kind::Null => crate::types::PublicKind::Null,
			Kind::Bool => crate::types::PublicKind::Bool,
			Kind::Bytes => crate::types::PublicKind::Bytes,
			Kind::Datetime => crate::types::PublicKind::Datetime,
			Kind::Date => crate::types::PublicKind::Date,
			Kind::Time => crate::types::PublicKind::Time,
			Kind::Timestamp => crate::types::PublicKind::Timestamp,
			Kind::Interval => crate::types::PublicKind::Interval,
			Kind::Decimal => crate::types::PublicKind::Decimal,
			Kind::BigDecimal => crate::types::PublicKind::BigDecimal,
			Kind::Duration => crate::types::PublicKind::Duration,
			Kind::Float => crate::types::PublicKind::Float,
			Kind::Int => crate::types::PublicKind::Int,
			Kind::Number => crate::types::PublicKind::Number,
			Kind::Object => crate::types::PublicKind::Object,
			Kind::String => crate::types::PublicKind::String,
			Kind::Uuid => crate::types::PublicKind::Uuid,
			Kind::Regex => crate::types::PublicKind::Regex,
			Kind::Range => crate::types::PublicKind::Range,
			Kind::Table(tables) => crate::types::PublicKind::Table(
				tables.into_iter().map(IntoPublicTable::into_public_table).collect(),
			),
			Kind::Record(tables) => crate::types::PublicKind::Record(
				tables.into_iter().map(IntoPublicTable::into_public_table).collect(),
			),
			Kind::Geometry(kinds) => {
				crate::types::PublicKind::Geometry(kinds.into_iter().map(Into::into).collect())
			}
			Kind::Either(kinds) => {
				crate::types::PublicKind::Either(kinds.into_iter().map(Into::into).collect())
			}
			Kind::Set(kind, size) => crate::types::PublicKind::Set(Box::new((*kind).into()), size),
			Kind::Array(kind, size) => {
				crate::types::PublicKind::Array(Box::new((*kind).into()), size)
			}
			Kind::Function(args, ret) => crate::types::PublicKind::Function(
				args.map(|a| a.into_iter().map(Into::into).collect()),
				ret.map(|r| Box::new((*r).into())),
			),
			Kind::File(bucket) => crate::types::PublicKind::File(bucket),
			Kind::Enum(labels) => crate::types::PublicKind::Enum(labels),
			Kind::Named(name) => crate::types::PublicKind::Named(name),
			Kind::Literal(lit) => crate::types::PublicKind::Literal(lit.into()),
			Kind::I8 => crate::types::PublicKind::I8,
			Kind::U8 => crate::types::PublicKind::U8,
			Kind::I16 => crate::types::PublicKind::I16,
			Kind::U16 => crate::types::PublicKind::U16,
			Kind::I32 => crate::types::PublicKind::I32,
			Kind::U32 => crate::types::PublicKind::U32,
			Kind::U64 => crate::types::PublicKind::U64,
			Kind::F16 => crate::types::PublicKind::F16,
			Kind::F32 => crate::types::PublicKind::F32,
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug)]
pub enum KindLiteral {
	String(Strand),
	Integer(i64),
	Float(f64),
	Decimal(Decimal),
	Duration(Duration),
	Array(Vec<Kind>),
	Object(BTreeMap<Strand, Kind>),
	// This variant is just for a performance optimization.
	// Should probably be removed when we have a planner.
	//DiscriminatedObject(String, Vec<BTreeMap<String, Kind>>),
	Bool(bool),
}

impl From<crate::types::PublicKindLiteral> for KindLiteral {
	fn from(v: crate::types::PublicKindLiteral) -> Self {
		match v {
			crate::types::PublicKindLiteral::String(s) => KindLiteral::String(s.into()),
			crate::types::PublicKindLiteral::Integer(i) => KindLiteral::Integer(i),
			crate::types::PublicKindLiteral::Float(f) => KindLiteral::Float(f),
			crate::types::PublicKindLiteral::Decimal(d) => KindLiteral::Decimal(d),
			crate::types::PublicKindLiteral::Duration(d) => {
				KindLiteral::Duration(crate::val::Duration(*d))
			}
			crate::types::PublicKindLiteral::Array(kinds) => {
				KindLiteral::Array(kinds.into_iter().map(Kind::from).collect())
			}
			crate::types::PublicKindLiteral::Object(obj) => KindLiteral::Object(
				obj.into_iter().map(|(k, v)| (k.into(), Kind::from(v))).collect(),
			),
			crate::types::PublicKindLiteral::Bool(b) => KindLiteral::Bool(b),
		}
	}
}

impl From<KindLiteral> for crate::types::PublicKindLiteral {
	fn from(v: KindLiteral) -> Self {
		match v {
			KindLiteral::String(s) => crate::types::PublicKindLiteral::String(s.into_string()),
			KindLiteral::Integer(i) => crate::types::PublicKindLiteral::Integer(i),
			KindLiteral::Float(f) => crate::types::PublicKindLiteral::Float(f),
			KindLiteral::Decimal(d) => crate::types::PublicKindLiteral::Decimal(d),
			KindLiteral::Duration(d) => {
				crate::types::PublicKindLiteral::Duration(crate::types::PublicDuration::from(d.0))
			}
			KindLiteral::Array(kinds) => {
				crate::types::PublicKindLiteral::Array(kinds.into_iter().map(Into::into).collect())
			}
			KindLiteral::Object(obj) => crate::types::PublicKindLiteral::Object(
				obj.into_iter().map(|(k, v)| (k.into_string(), v.into())).collect(),
			),
			KindLiteral::Bool(b) => crate::types::PublicKindLiteral::Bool(b),
		}
	}
}

impl PartialEq for KindLiteral {
	fn eq(&self, other: &Self) -> bool {
		match self {
			KindLiteral::String(strand) => {
				if let KindLiteral::String(other) = other {
					strand == other
				} else {
					false
				}
			}
			KindLiteral::Integer(x) => {
				if let KindLiteral::Integer(other) = other {
					x == other
				} else {
					false
				}
			}
			KindLiteral::Float(x) => {
				if let KindLiteral::Float(other) = other {
					x.to_bits() == other.to_bits()
				} else {
					false
				}
			}
			KindLiteral::Decimal(decimal) => {
				if let KindLiteral::Decimal(other) = other {
					decimal == other
				} else {
					false
				}
			}
			KindLiteral::Duration(duration) => {
				if let KindLiteral::Duration(other) = other {
					duration == other
				} else {
					false
				}
			}
			KindLiteral::Array(kinds) => {
				if let KindLiteral::Array(other) = other {
					kinds == other
				} else {
					false
				}
			}
			KindLiteral::Object(btree_map) => {
				if let KindLiteral::Object(other) = other {
					btree_map == other
				} else {
					false
				}
			}
			/*
			KindLiteral::DiscriminatedObject(a, b) => {
				if let KindLiteral::DiscriminatedObject(c, d) = other {
					a == c && b == d
				} else {
					false
				}
			}
			*/
			KindLiteral::Bool(a) => {
				if let KindLiteral::Bool(b) = other {
					a == b
				} else {
					false
				}
			}
		}
	}
}
impl Eq for KindLiteral {}
impl Hash for KindLiteral {
	fn hash<H: Hasher>(&self, state: &mut H) {
		std::mem::discriminant(self).hash(state);
		match self {
			KindLiteral::String(strand) => strand.hash(state),
			KindLiteral::Integer(x) => x.hash(state),
			KindLiteral::Float(x) => x.to_bits().hash(state),
			KindLiteral::Decimal(decimal) => decimal.hash(state),
			KindLiteral::Duration(duration) => duration.hash(state),
			KindLiteral::Array(kinds) => kinds.hash(state),
			KindLiteral::Object(btree_map) => btree_map.hash(state),
			/*
			KindLiteral::DiscriminatedObject(a, b) => {
				a.hash(state);
				b.hash(state);
			}
			*/
			KindLiteral::Bool(x) => x.hash(state),
		}
	}
}

impl KindLiteral {
	pub fn to_kind(&self) -> Kind {
		match self {
			Self::String(_) => Kind::String,
			Self::Integer(_) | Self::Float(_) | Self::Decimal(_) => Kind::Number,
			Self::Duration(_) => Kind::Duration,
			Self::Array(a) => {
				if let Some(inner) = a.first()
					&& a.iter().all(|x| x == inner)
				{
					return Kind::Array(Box::new(inner.to_owned()), Some(a.len() as u64));
				}

				Kind::Array(Box::new(Kind::Any), None)
			}
			Self::Object(_) => Kind::Object,
			//Self::DiscriminatedObject(_, _) => Kind::Object,
			Self::Bool(_) => Kind::Bool,
		}
	}

	pub fn validate_value(&self, value: &Value) -> bool {
		match self {
			Self::String(v) => value.is_strand_and(|s| s == v),
			Self::Integer(v) => value.is_number_and(|n| *n == Number::Int(*v)),
			Self::Float(v) => value.is_number_and(|n| *n == Number::Float(*v)),
			Self::Decimal(v) => value.is_number_and(|n| *n == Number::Decimal(*v)),
			Self::Duration(v) => value.is_duration_and(|n| n == v),
			Self::Bool(v) => value.is_bool_and(|b| b == v),
			Self::Array(a) => value.is_array_and(|x| {
				if a.len() != x.len() {
					return false;
				}

				for (i, inner) in a.iter().enumerate() {
					if let Some(value) = x.get(i) {
						if !value.can_coerce_to_kind(inner) {
							return false;
						}
					} else {
						return false;
					}
				}

				true
			}),
			Self::Object(lit) => value.is_object_and(|val| {
				let mut lit_iter = lit.iter();
				let mut val_iter = val.iter();

				let mut lit_next = lit_iter.next();
				let mut val_next = val_iter.next();

				while lit_next.is_some() || val_next.is_some() {
					match (lit_next, val_next) {
						(Some((lit_k, lit_kind)), Some((val_k, val_v))) => {
							match lit_k.as_str().cmp(val_k.as_str()) {
								Ordering::Less => {
									// Key in type but not in value - check if optional
									if !lit_kind.can_be_none() {
										return false;
									}
									lit_next = lit_iter.next();
								}
								Ordering::Equal => {
									// Key in both - validate type
									if !val_v.can_coerce_to_kind(lit_kind) {
										return false;
									}
									lit_next = lit_iter.next();
									val_next = val_iter.next();
								}
								Ordering::Greater => {
									// Key in value but not in type - extra key!
									return false;
								}
							}
						}
						(Some((_, lit_kind)), None) => {
							// Remaining keys in type but not in value - check if optional
							if !lit_kind.can_be_none() {
								return false;
							}
							lit_next = lit_iter.next();
						}
						(None, Some(_)) => {
							// Remaining keys in value but not in type - extra keys!
							return false;
						}
						(None, None) => break,
					}
				}

				true
			}),
			/*
			Self::DiscriminatedObject(key, discriminants) => match value {
				Value::Object(x) => {
					let Some(value) = x.get(key) else {
						return false;
					};
					if let Some(o) =
						discriminants.iter().find(|o| value.can_coerce_to_kind(&o[key]))
					{
						if o.len() < x.len() {
							return false;
						}

						for (k, v) in o.iter() {
							if let Some(value) = x.get(k) {
								if !value.can_coerce_to_kind(v) {
									return false;
								}
							} else if !v.can_be_none() {
								return false;
							}
						}

						true
					} else {
						false
					}
				}
				_ => false,
			},
			*/
		}
	}

	pub fn allows_nested_kind(&self, path: &[Part], kind: &Kind) -> bool {
		// ANY type won't cause a mismatch
		if kind.is_any() {
			return true;
		}

		// We reached the end of the path
		// Check if the literal is equal to the kind
		if path.is_empty() {
			return if let Kind::Literal(lit) = kind {
				self == lit
			} else {
				&self.to_kind() == kind
			};
		}

		match self {
			KindLiteral::Array(x) => match path.first() {
				Some(Part::All) => x.iter().all(|y| y.allows_nested_kind(&path[1..], kind)),
				Some(part) => part
					.as_old_index()
					.and_then(|idx| x.get(idx))
					.map(|x| x.allows_nested_kind(&path[1..], kind))
					.unwrap_or(false),
				None => false,
			},
			KindLiteral::Object(x) => match path.first() {
				Some(Part::All) => x.iter().all(|(_, y)| y.allows_nested_kind(&path[1..], kind)),
				Some(Part::Field(k)) => {
					if let Some(y) = x.get(k.as_str()) {
						y.allows_nested_kind(&path[1..], kind)
					} else {
						false
					}
				}
				_ => false,
			},
			/*
			KindLiteral::DiscriminatedObject(_, discriminants) => match path.first() {
				Some(Part::All) => discriminants
					.iter()
					.all(|o| o.iter().all(|(_, y)| y.allows_nested_kind(&path[1..], kind))),
				Some(Part::Field(k)) => discriminants.iter().all(|o| {
					if let Some(y) = o.get(&**k) {
						y.allows_nested_kind(&path[1..], kind)
					} else {
						false
					}
				}),
				_ => false,
			},
			*/
			KindLiteral::String(_)
			| KindLiteral::Integer(_)
			| KindLiteral::Float(_)
			| KindLiteral::Decimal(_)
			| KindLiteral::Duration(_)
			| KindLiteral::Bool(_) => false,
		}
	}
}

impl ToSql for KindLiteral {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			KindLiteral::String(s) => write_sql!(f, fmt, "{}", QuoteStr(s)),
			KindLiteral::Integer(n) => write_sql!(f, fmt, "{}", n),
			KindLiteral::Float(n) => write_sql!(f, fmt, " {}", Float(*n)),
			KindLiteral::Decimal(n) => write_sql!(f, fmt, " {}", n),
			KindLiteral::Duration(d) => write_sql!(f, fmt, "{}", SqlDuration(d.0)),
			KindLiteral::Bool(b) => write_sql!(f, fmt, "{}", b),
			KindLiteral::Array(a) => {
				f.push('[');
				if !a.is_empty() {
					let fmt = fmt.increment();
					write_sql!(f, fmt, "{}", Fmt::pretty_comma_separated(a.as_slice()));
				}
				f.push(']');
			}
			KindLiteral::Object(o) => {
				if fmt.is_pretty() {
					f.push('{');
				} else {
					f.push_str("{ ");
				}
				if !o.is_empty() {
					let fmt = fmt.increment();
					write_sql!(
						f,
						fmt,
						"{}",
						Fmt::pretty_comma_separated(o.iter().map(|args| Fmt::new(
							args,
							|(k, v), f, fmt| {
								write_sql!(f, fmt, "{}: {}", EscapeObjectKey(k), v)
							}
						)),)
					);
				}
				if fmt.is_pretty() {
					f.push('}');
				} else {
					f.push_str(" }");
				}
			}
		}
	}
}

#[cfg(test)]
mod reference_target_tests {
	use super::*;

	fn tb(name: &str) -> TableName {
		name.into()
	}

	#[test]
	fn typed_record_targets_only_its_table() {
		let k = Kind::Record(vec![tb("person")]);
		assert!(k.reference_can_target(&tb("person")));
		assert!(!k.reference_can_target(&tb("comment")));
	}

	#[test]
	fn untyped_record_targets_any_table() {
		// `record` / `record<>` has no table constraint, so it can hold a
		// record of any table and must never let the scan be skipped.
		let k = Kind::Record(Vec::new());
		assert!(k.reference_can_target(&tb("person")));
		assert!(k.reference_can_target(&tb("anything")));
	}

	#[test]
	fn multi_table_record_targets_each() {
		let k = Kind::Record(vec![tb("person"), tb("robot")]);
		assert!(k.reference_can_target(&tb("person")));
		assert!(k.reference_can_target(&tb("robot")));
		assert!(!k.reference_can_target(&tb("comment")));
	}

	#[test]
	fn array_and_set_of_records() {
		let arr = Kind::Array(Box::new(Kind::Record(vec![tb("house")])), None);
		assert!(arr.reference_can_target(&tb("house")));
		assert!(!arr.reference_can_target(&tb("person")));

		let set = Kind::Set(Box::new(Kind::Record(vec![tb("house")])), None);
		assert!(set.reference_can_target(&tb("house")));
		assert!(!set.reference_can_target(&tb("person")));
	}

	#[test]
	fn option_record_is_either_none_record() {
		// `option<record<person>>` is represented as `Either([None, Record([person])])`.
		let k = Kind::Either(vec![Kind::None, Kind::Record(vec![tb("person")])]);
		assert!(k.reference_can_target(&tb("person")));
		assert!(!k.reference_can_target(&tb("comment")));
	}

	#[test]
	fn either_of_records_targets_each() {
		let k =
			Kind::Either(vec![Kind::Record(vec![tb("person")]), Kind::Record(vec![tb("robot")])]);
		assert!(k.reference_can_target(&tb("person")));
		assert!(k.reference_can_target(&tb("robot")));
		assert!(!k.reference_can_target(&tb("comment")));
	}

	#[test]
	fn literal_array_of_records() {
		let k = Kind::Literal(KindLiteral::Array(vec![Kind::Record(vec![tb("a")])]));
		assert!(k.reference_can_target(&tb("a")));
		assert!(!k.reference_can_target(&tb("b")));
	}

	#[test]
	fn nested_option_array_record() {
		// `option<array<record<house>>>` => `Either([None, Array(Record([house]))])`.
		let k = Kind::Either(vec![
			Kind::None,
			Kind::Array(Box::new(Kind::Record(vec![tb("house")])), None),
		]);
		assert!(k.reference_can_target(&tb("house")));
		assert!(!k.reference_can_target(&tb("person")));
	}

	#[test]
	fn non_record_kinds_never_target() {
		assert!(!Kind::String.reference_can_target(&tb("person")));
		assert!(!Kind::Int.reference_can_target(&tb("person")));
		assert!(!Kind::None.reference_can_target(&tb("person")));
		assert!(!Kind::Object.reference_can_target(&tb("person")));
	}

	#[test]
	fn any_kind_matches_defensively() {
		// `Kind::Any` is rejected for reference fields at DEFINE time, but the
		// walker treats it as a match so the scan still runs if it ever occurs.
		assert!(Kind::Any.reference_can_target(&tb("person")));
	}

	fn collect(kind: &Kind) -> (bool, HashSet<TableName>) {
		let mut out = HashSet::new();
		let unbounded = kind.collect_reference_target_tables(&mut out);
		(unbounded, out)
	}

	#[test]
	fn collect_typed_record() {
		let (unbounded, tables) = collect(&Kind::Record(vec![tb("person")]));
		assert!(!unbounded);
		assert_eq!(tables, HashSet::from([tb("person")]));
	}

	#[test]
	fn collect_untyped_record_is_unbounded() {
		let (unbounded, _) = collect(&Kind::Record(Vec::new()));
		assert!(unbounded);
	}

	#[test]
	fn collect_either_union_and_containers() {
		// either<array<record<a>> | record<b>> => {a, b}
		let k = Kind::Either(vec![
			Kind::Array(Box::new(Kind::Record(vec![tb("a")])), None),
			Kind::Record(vec![tb("b")]),
		]);
		let (unbounded, tables) = collect(&k);
		assert!(!unbounded);
		assert_eq!(tables, HashSet::from([tb("a"), tb("b")]));
	}

	#[test]
	fn collect_mixed_typed_and_untyped_is_unbounded() {
		// A union that includes an untyped record is unbounded, even though the
		// typed arm's table is still collected.
		let k = Kind::Either(vec![Kind::Record(vec![tb("a")]), Kind::Record(Vec::new())]);
		let (unbounded, tables) = collect(&k);
		assert!(unbounded);
		assert!(tables.contains(&tb("a")));
	}

	#[test]
	fn collect_non_record_is_empty() {
		let (unbounded, tables) = collect(&Kind::String);
		assert!(!unbounded);
		assert!(tables.is_empty());
	}
}

/// Exhaustive-over-variants render/parse round-trip corpus for `Kind`.
///
/// Catalog definitions store kinds as canonical SurrealQL text
/// (`Kind::to_sql()`) and re-parse via `syn::kind_for_definition()` on read.
/// Every variant (including recursive nestings) must survive that round trip
/// byte-for-byte in structure, or a stored `DEFINE FIELD ... TYPE ...` would
/// silently change meaning.
#[cfg(test)]
mod round_trip_tests {
	use rstest::rstest;
	use surrealdb_types::ToSql;

	use super::*;

	fn tb(name: &str) -> TableName {
		name.into()
	}

	fn round_trip(k: &Kind) {
		let text = k.to_sql();
		let reparsed: Kind = crate::parser::kind_for_definition(&text)
			.unwrap_or_else(|e| panic!("failed to reparse kind text {text:?}: {e}"));
		assert_eq!(&reparsed, k, "round trip mismatch via text {text:?}");
	}

	#[rstest]
	#[case::any(Kind::Any)]
	#[case::none(Kind::None)]
	#[case::null(Kind::Null)]
	#[case::bool(Kind::Bool)]
	#[case::bytes(Kind::Bytes)]
	#[case::datetime(Kind::Datetime)]
	#[case::decimal(Kind::Decimal)]
	#[case::duration(Kind::Duration)]
	#[case::float(Kind::Float)]
	#[case::int(Kind::Int)]
	#[case::i8(Kind::I8)]
	#[case::u8(Kind::U8)]
	#[case::i16(Kind::I16)]
	#[case::u16(Kind::U16)]
	#[case::i32(Kind::I32)]
	#[case::u32(Kind::U32)]
	#[case::u64(Kind::U64)]
	#[case::f16(Kind::F16)]
	#[case::f32(Kind::F32)]
	#[case::number(Kind::Number)]
	#[case::object(Kind::Object)]
	#[case::string(Kind::String)]
	#[case::uuid(Kind::Uuid)]
	#[case::regex(Kind::Regex)]
	#[case::range(Kind::Range)]
	#[case::table_untyped(Kind::Table(vec![]))]
	#[case::table_typed(Kind::Table(vec![tb("person"), tb("robot")]))]
	#[case::record_untyped(Kind::Record(vec![]))]
	#[case::record_typed(Kind::Record(vec![tb("person")]))]
	#[case::geometry_untyped(Kind::Geometry(vec![]))]
	#[case::geometry_typed(Kind::Geometry(vec![GeometryKind::Point, GeometryKind::MultiPolygon]))]
	#[case::either(Kind::Either(vec![Kind::String, Kind::Int, Kind::None]))]
	#[case::option_record(Kind::Either(vec![Kind::None, Kind::Record(vec![tb("person")])]))]
	#[case::set_unbounded(Kind::Set(Box::new(Kind::String), None))]
	#[case::set_bounded(Kind::Set(Box::new(Kind::Int), Some(10)))]
	#[case::array_unbounded(Kind::Array(Box::new(Kind::Any), None))]
	#[case::array_bounded(Kind::Array(Box::new(Kind::Record(vec![tb("a")])), Some(3)))]
	#[case::nested_array(Kind::Array(Box::new(Kind::Array(Box::new(Kind::Int), None)), None))]
	// `Kind::Function`'s argument/return payload has no surface grammar (the
	// parser only ever produces `Function(None, None)` from the `function`
	// keyword, and `ToSql` renders every payload as just `"function"`) — this
	// predates the catalog text migration and is unrelated to it, so only the
	// parseable/renderable shape is covered here.
	#[case::function_untyped(Kind::Function(None, None))]
	#[case::file_untyped(Kind::File(vec![]))]
	#[case::file_typed(Kind::File(vec!["bucket1".to_string(), "bucket2".to_string()]))]
	#[case::literal_string(Kind::Literal(KindLiteral::String(Strand::new_static("a"))))]
	#[case::literal_integer(Kind::Literal(KindLiteral::Integer(42)))]
	#[case::literal_float(Kind::Literal(KindLiteral::Float(1.5)))]
	#[case::literal_decimal(Kind::Literal(KindLiteral::Decimal(Decimal::new(125, 2))))]
	#[case::literal_duration(Kind::Literal(KindLiteral::Duration(Duration(
		std::time::Duration::from_secs(3600)
	))))]
	#[case::literal_bool(Kind::Literal(KindLiteral::Bool(true)))]
	#[case::literal_array(Kind::Literal(KindLiteral::Array(vec![Kind::String, Kind::Int])))]
	#[case::literal_object(Kind::Literal(KindLiteral::Object(BTreeMap::from([(
		Strand::new_static("a"),
		Kind::String,
	)]))))]
	#[case::deeply_nested(Kind::Either(vec![
		Kind::Array(Box::new(Kind::Record(vec![tb("a"), tb("b")])), Some(5)),
		Kind::Set(Box::new(Kind::Literal(KindLiteral::Integer(1))), None),
		Kind::None,
	]))]
	fn kind_round_trips_through_text(#[case] k: Kind) {
		round_trip(&k);
	}
}

#[cfg(test)]
mod widening_tests {
	use super::*;
	use crate::val::Value;
	use crate::val::convert_public::convert_public_value_to_internal;

	const KINDS: &[&str] = &[
		"any",
		"none",
		"null",
		"bool",
		"int",
		"i8",
		"u8",
		"i16",
		"u16",
		"i32",
		"u32",
		"u64",
		"float",
		"f16",
		"f32",
		"decimal",
		"number",
		"string",
		"datetime",
		"duration",
		"uuid",
		"object",
		"option<int>",
		"option<string>",
		"int | string",
		"string | int",
		"int | float",
		"float | int",
		"number | string",
		"array",
		"array<int>",
		"array<float>",
		"array<number>",
		"array<int, 2>",
		"array<option<int>>",
		"set<int>",
		"set<number>",
		"record",
		"record<a>",
		"record<a | b>",
		"{ a: int }",
		"{ a: number }",
		"{ a: int, b: option<string> }",
		"'a' | 'b'",
		"'a' | 'b' | 'c'",
		"'b' | 'a'",
		"'c' | 'a' | 'b'",
		"1 | 2",
		"1 | 2 | 3",
		"3 | 1 | 2",
		"1 | 'a'",
		"int | 'a'",
		"'a' | int",
	];

	const VALUES: &[&str] = &[
		"NONE",
		"NULL",
		"true",
		"0",
		"1",
		"-7",
		"9007199254740993",
		"1.5f",
		"2f",
		"1e30f",
		"3dec",
		"'a'",
		"'1'",
		"'b'",
		"d'2020-01-01T00:00:00Z'",
		"1s",
		"u'00000000-0000-0000-0000-000000000001'",
		"[]",
		"[1, 2]",
		"[1.5f, 2]",
		"[1, NONE]",
		"{1, 2}",
		"{}",
		"{ a: 1 }",
		"{ a: 1.5f }",
		"{ a: 1, b: 'x' }",
		"{ a: 1, c: 2 }",
		"a:1",
		"b:1",
		"c:1",
	];

	fn kind(text: &str) -> Kind {
		crate::syn::kind_for_definition(text)
			.unwrap_or_else(|e| panic!("the kind {text:?} did not parse: {e}"))
	}

	fn value(text: &str) -> Value {
		convert_public_value_to_internal(
			crate::syn::value(text)
				.unwrap_or_else(|e| panic!("the value {text:?} did not parse: {e}")),
		)
	}

	/// `widens_to` is a promise about coercion: every value that conforms to
	/// the old kind, one coercion to it returns unchanged, also conforms to
	/// the new kind unchanged. Checked over every pair of a matrix of kinds
	/// and values.
	#[test]
	fn a_widening_keeps_every_conforming_value_unchanged() {
		let kinds: Vec<(&str, Kind)> = KINDS.iter().map(|text| (*text, kind(text))).collect();
		let values: Vec<(&str, Value)> = VALUES.iter().map(|text| (*text, value(text))).collect();
		let mut failures = Vec::new();
		for (old_text, old) in &kinds {
			for (new_text, new) in &kinds {
				if !old.widens_to(new) {
					continue;
				}
				for (value_text, value) in &values {
					let conforms = value.clone().coerce_to_kind(old).is_ok_and(|v| v == *value);
					if !conforms {
						continue;
					}
					let kept = value.clone().coerce_to_kind(new).is_ok_and(|v| v == *value);
					if !kept {
						failures.push(format!("{old_text} -> {new_text} changes {value_text}"));
					}
				}
			}
		}
		assert!(failures.is_empty(), "widenings that do not hold:\n{}", failures.join("\n"));
	}

	#[test]
	fn the_obvious_widenings_are_recognised() {
		for (old, new) in [
			("int", "number"),
			("int", "option<int>"),
			("int", "int | string"),
			("int", "any"),
			("array<int>", "array<number>"),
			("'a' | 'b'", "'a' | 'b' | 'c'"),
			("'a' | 'b'", "'c' | 'a' | 'b'"),
			("1 | 2", "1 | 2 | 3"),
			("date", "date | datetime"),
			("mood", "option<mood>"),
		] {
			assert!(kind(old).widens_to(&kind(new)), "{old} -> {new}");
		}
		for (old, new) in [
			("int", "float"),
			("int", "float | int"),
			("option<int>", "int"),
			("'a' | 'b'", "'a'"),
			("'a' | 'b' | 'c'", "'a' | 'b'"),
			("1 | 2", "2"),
			// The earlier arm coerces the value, since each of these coerces from the others.
			("date", "datetime | date"),
			("datetime", "timestamp | datetime"),
			("timestamp", "date | timestamp"),
			// A named type may coerce any value, so an arm before the one it widens to may take
			// it.
			("int", "mood | int"),
		] {
			assert!(!kind(old).widens_to(&kind(new)), "{old} -> {new}");
		}
	}
}

#[cfg(test)]
mod kind_revision_tests {
	use revision::{DeserializeRevisioned, SerializeRevisioned};

	use super::*;

	#[test]
	fn kind_revision_round_trip() {
		let kinds = [
			Kind::Any,
			Kind::None,
			Kind::Null,
			Kind::Bool,
			Kind::Bytes,
			Kind::Datetime,
			Kind::Decimal,
			Kind::Duration,
			Kind::Float,
			Kind::Int,
			Kind::Number,
			Kind::Object,
			Kind::String,
			Kind::Uuid,
			Kind::Regex,
			Kind::Range,
			Kind::I8,
			Kind::U8,
			Kind::I16,
			Kind::U16,
			Kind::I32,
			Kind::U32,
			Kind::U64,
			Kind::F16,
			Kind::F32,
			Kind::Date,
			Kind::Time,
			Kind::Timestamp,
			Kind::Interval,
		];
		for k in &kinds {
			let mut buf = Vec::new();
			k.serialize_revisioned(&mut buf).unwrap();
			let mut slice = buf.as_slice();
			let decoded = Kind::deserialize_revisioned(&mut slice).unwrap();
			assert_eq!(k, &decoded);
		}
		for (k, tag) in
			[(Kind::Date, 34u32), (Kind::Time, 35), (Kind::Timestamp, 36), (Kind::Interval, 37)]
		{
			let mut buf = Vec::new();
			k.serialize_revisioned(&mut buf).unwrap();
			let mut expected = Vec::new();
			2u16.serialize_revisioned(&mut expected).unwrap();
			tag.serialize_revisioned(&mut expected).unwrap();
			assert_eq!(buf, expected);
		}
	}
}
