use crate::sql::{fmt::Fmt, Table};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 1)]
#[non_exhaustive]
pub enum Kind {
	Any,
	Null,
	Bool,
	Bytes,
	Datetime,
	Decimal,
	Duration,
	Float,
	Int,
	Number,
	Object,
	Point,
	String,
	Uuid,
	Record(Vec<Table>),
	Geometry(Vec<String>),
	Option(Box<Kind>),
	Either(Vec<Kind>),
	Set(Box<Kind>, Option<u64>),
	Array(Box<Kind>, Option<u64>),
}

impl Default for Kind {
	fn default() -> Self {
		Self::Any
	}
}

impl Kind {
	fn is_any(&self) -> bool {
		matches!(self, Kind::Any)
	}

	// return the kind of the contained value.
	//
	// For example: for `array<number>` or `set<number>` this returns `number`.
	// For `array<number> | set<float>` this returns `number | float`.
	pub(crate) fn inner_kind(&self) -> Option<Kind> {
		let mut this = self;
		loop {
			match &this {
				Kind::Any
				| Kind::Null
				| Kind::Bool
				| Kind::Bytes
				| Kind::Datetime
				| Kind::Decimal
				| Kind::Duration
				| Kind::Float
				| Kind::Int
				| Kind::Number
				| Kind::Object
				| Kind::Point
				| Kind::String
				| Kind::Uuid
				| Kind::Record(_)
				| Kind::Geometry(_) => return None,
				Kind::Option(x) => {
					this = x;
				}
				Kind::Array(x, _) | Kind::Set(x, _) => return Some(x.as_ref().clone()),
				Kind::Either(x) => {
					// a either shouldn't be able to contain a either itself so recursing here
					// should be fine.
					let kinds: Vec<Kind> = x.iter().filter_map(Self::inner_kind).collect();
					if kinds.is_empty() {
						return None;
					}
					return Some(Kind::Either(kinds));
				}
			}
		}
	}
}

impl From<&Kind> for Box<Kind> {
	#[inline]
	fn from(v: &Kind) -> Self {
		Box::new(v.clone())
	}
}

impl Display for Kind {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Kind::Any => f.write_str("any"),
			Kind::Null => f.write_str("null"),
			Kind::Bool => f.write_str("bool"),
			Kind::Bytes => f.write_str("bytes"),
			Kind::Datetime => f.write_str("datetime"),
			Kind::Decimal => f.write_str("decimal"),
			Kind::Duration => f.write_str("duration"),
			Kind::Float => f.write_str("float"),
			Kind::Int => f.write_str("int"),
			Kind::Number => f.write_str("number"),
			Kind::Object => f.write_str("object"),
			Kind::Point => f.write_str("point"),
			Kind::String => f.write_str("string"),
			Kind::Uuid => f.write_str("uuid"),
			Kind::Option(k) => write!(f, "option<{}>", k),
			Kind::Record(k) => match k {
				k if k.is_empty() => write!(f, "record"),
				k => write!(f, "record<{}>", Fmt::verbar_separated(k)),
			},
			Kind::Geometry(k) => match k {
				k if k.is_empty() => write!(f, "geometry"),
				k => write!(f, "geometry<{}>", Fmt::verbar_separated(k)),
			},
			Kind::Set(k, l) => match (k, l) {
				(k, None) if k.is_any() => write!(f, "set"),
				(k, None) => write!(f, "set<{k}>"),
				(k, Some(l)) => write!(f, "set<{k}, {l}>"),
			},
			Kind::Array(k, l) => match (k, l) {
				(k, None) if k.is_any() => write!(f, "array"),
				(k, None) => write!(f, "array<{k}>"),
				(k, Some(l)) => write!(f, "array<{k}, {l}>"),
			},
			Kind::Either(k) => write!(f, "{}", Fmt::verbar_separated(k)),
		}
	}
}
