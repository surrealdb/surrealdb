mod geometry;
mod literal;

use std::collections::HashSet;
use std::fmt::Display;

pub use geometry::*;
pub use literal::*;
use serde::{Deserialize, Serialize};
use surrealdb_types_derive::write_sql;

use crate::utils::display::format_seperated;
use crate::utils::escape::QuoteStr;
use crate::{self as surrealdb_types, SqlFormat, Table, ToSql};

/// The kind of a SurrealDB value.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq, Serialize, Deserialize)]
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
	/// Calendar date type, without a time zone.
	Date,
	/// Time of day type, without a date or time zone.
	Time,
	/// Date and time of day type, without a time zone.
	Timestamp,
	/// Calendar-aware interval type of months, days, and microseconds.
	Interval,
	/// Decimal type.
	Decimal,
	/// Duration type.
	Duration,
	/// 16-bit floating point type.
	F16,
	/// 32-bit floating point type.
	F32,
	/// 64-bit floating point type.
	Float,
	/// 8-bit signed integer type.
	I8,
	/// 16-bit signed integer type.
	I16,
	/// 32-bit signed integer type.
	I32,
	/// 64-bit signed integer type.
	Int,
	/// 8-bit unsigned integer type.
	U8,
	/// 16-bit unsigned integer type.
	U16,
	/// 32-bit unsigned integer type.
	U32,
	/// 64-bit unsigned integer type.
	U64,
	/// Number type, can be any integer, float or decimal kind.
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
	Table(Vec<Table>),
	/// A record type.
	Record(Vec<Table>),
	/// A geometry type.
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
	/// An inline enum type: a string constrained to an ordered set of labels, in declaration
	/// order.
	Enum(Vec<String>),
	/// A type declared by `DEFINE TYPE`, by name.
	Named(String),
	/// Arbitrary-precision decimal type, with no bound on digits or scale.
	BigDecimal,
}

impl Kind {
	/// Recursively flatten a kind into a vector of kinds.
	pub fn flatten(self) -> Vec<Kind> {
		if let Kind::Either(x) = self {
			x.into_iter().flat_map(|k| k.flatten()).collect()
		} else {
			vec![self]
		}
	}

	/// Create an either kind from a vector of kinds.
	/// If after dedeplication the vector is empty, return `Kind::None`.
	/// If after dedeplication the vector has one element, return that element.
	/// If after dedeplication the vector has multiple elements, return an `Either` kind with the
	/// elements.
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

	/// Create an option kind from a kind.
	pub fn option(kind: Kind) -> Kind {
		Kind::either(vec![Kind::None, kind])
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

	/// All numeric kinds (`int`, `float`, `decimal`, `bigdecimal`, `number`, and all
	/// [`Self::SIZED`] kinds).
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

	/// Returns `true` if this kind admits only numbers (`int`, `float`, `decimal`, `bigdecimal`,
	/// `number`, or a sized numeric kind).
	pub const fn is_numeric(&self) -> bool {
		matches!(self, Kind::Int | Kind::Float | Kind::Decimal | Kind::BigDecimal | Kind::Number)
			|| self.is_sized_number()
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
			Kind::F16 => f.push_str("f16"),
			Kind::F32 => f.push_str("f32"),
			Kind::Float => f.push_str("float"),
			Kind::I8 => f.push_str("i8"),
			Kind::I16 => f.push_str("i16"),
			Kind::I32 => f.push_str("i32"),
			Kind::Int => f.push_str("int"),
			Kind::U8 => f.push_str("u8"),
			Kind::U16 => f.push_str("u16"),
			Kind::U32 => f.push_str("u32"),
			Kind::U64 => f.push_str("u64"),
			Kind::Number => f.push_str("number"),
			Kind::Object => f.push_str("object"),
			Kind::String => f.push_str("string"),
			Kind::Uuid => f.push_str("uuid"),
			Kind::Regex => f.push_str("regex"),
			Kind::Table(tables) => {
				if tables.is_empty() {
					f.push_str("table")
				} else {
					write_sql!(f, fmt, "table<{}>", format_seperated(tables, " | "));
				}
			}
			Kind::Record(table) => {
				if table.is_empty() {
					f.push_str("record")
				} else {
					write_sql!(f, fmt, "record<{}>", format_seperated(table, " | "))
				}
			}
			Kind::Geometry(kinds) => {
				if kinds.is_empty() {
					f.push_str("geometry")
				} else {
					write_sql!(f, fmt, "geometry<{}>", format_seperated(kinds, " | "))
				}
			}
			Kind::Either(kinds) => write_sql!(f, fmt, "{}", format_seperated(kinds, " | ")),
			Kind::Set(kind, max) => match max {
				Some(max) => write_sql!(f, fmt, "set<{}, {}>", kind, max),
				None => write_sql!(f, fmt, "set<{}>", kind),
			},
			Kind::Array(kind, max) => match max {
				Some(max) => write_sql!(f, fmt, "array<{}, {}>", kind, max),
				None => write_sql!(f, fmt, "array<{}>", kind),
			},
			Kind::Function(_, _) => f.push_str("function"),
			Kind::Range => f.push_str("range"),
			Kind::Literal(literal) => literal.fmt_sql(f, fmt),
			Kind::File(bucket) => {
				if bucket.is_empty() {
					f.push_str("file")
				} else {
					write_sql!(f, fmt, "file<{}>", format_seperated(bucket, " | "))
				}
			}
			Kind::Enum(labels) => {
				f.push_str("enum<");
				for (i, label) in labels.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					write_sql!(f, fmt, "{}", QuoteStr(label));
				}
				f.push('>');
			}
			Kind::Named(name) => {
				write_sql!(f, fmt, "{}", crate::utils::escape::EscapeSqonIdent(name));
			}
		}
	}
}

impl Display for Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str(&self.to_sql())
	}
}
