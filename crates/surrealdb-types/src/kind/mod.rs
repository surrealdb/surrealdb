mod geometry;
mod literal;

use std::fmt::Display;

pub use geometry::*;
pub use literal::*;
use serde::{Deserialize, Serialize};

use crate::utils::display::join_displayable;

/// The kind of a SurrealDB value.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum Kind {
	/// The most generic type, can be anything.
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
	/// A record type.
	Record(Vec<String>),
	/// A geometry type.
	/// The vec contains the geometry types as strings, for example `"point"` or
	/// `"polygon"`. TODO(3.0): Change to use an enum
	Geometry(Vec<GeometryKind>),
	/// An optional type.
	Option(Box<Kind>),
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
}

impl Default for Kind {
	fn default() -> Self {
		Self::Any
	}
}

impl Display for Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Kind::Any => write!(f, "any"),
			Kind::None => write!(f, "none"),
			Kind::Null => write!(f, "null"),
			Kind::Bool => write!(f, "bool"),
			Kind::Bytes => write!(f, "bytes"),
			Kind::Datetime => write!(f, "datetime"),
			Kind::Decimal => write!(f, "decimal"),
			Kind::Duration => write!(f, "duration"),
			Kind::Float => write!(f, "float"),
			Kind::Int => write!(f, "int"),
			Kind::Number => write!(f, "number"),
			Kind::Object => write!(f, "object"),
			Kind::String => write!(f, "string"),
			Kind::Uuid => write!(f, "uuid"),
			Kind::Regex => write!(f, "regex"),
			Kind::Record(table) => {
				if table.is_empty() {
					write!(f, "record")
				} else {
					write!(f, "record<{}>", join_displayable(table, " | "))
				}
			}
			Kind::Geometry(kinds) => {
				if kinds.is_empty() {
					write!(f, "geometry")
				} else {
					write!(f, "geometry<{}>", join_displayable(kinds, " | "))
				}
			}
			Kind::Option(kind) => write!(f, "option<{}>", kind),
			Kind::Either(kinds) => write!(f, "{}", join_displayable(kinds, " | ")),
			Kind::Set(kind, max) => match max {
				Some(max) => write!(f, "set<{}, {}>", kind, max),
				None => write!(f, "set<{}>", kind),
			},
			Kind::Array(kind, max) => match max {
				Some(max) => write!(f, "array<{}, {}>", kind, max),
				None => write!(f, "array<{}>", kind),
			},
			Kind::Function(_, _) => write!(f, "function"),
			Kind::Range => write!(f, "range"),
			Kind::Literal(literal) => write!(f, "{}", literal),
			Kind::File(bucket) => {
				if bucket.is_empty() {
					write!(f, "file")
				} else {
					write!(f, "file<{}>", join_displayable(bucket, " | "))
				}
			}
		}
	}
}
