use std::collections::BTreeMap;
use std::hash;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{Duration, Kind};

/// Represents literal values in SurrealDB's type system
///
/// Literal types are used to represent specific values that can only be a single value.
/// For example, a literal type `"a"` can only ever be the string `"a"`.
/// This is commonly used in `Kind::Either` to represent enum-like types.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum KindLiteral {
	/// A string literal
	String(String),
	/// An integer literal
	Integer(i64),
	/// A floating-point literal
	Float(f64),
	/// A decimal literal
	Decimal(Decimal),
	/// A duration literal
	Duration(Duration),
	/// An array of kinds literal
	Array(Vec<Kind>),
	/// An object with string keys and kind values literal
	Object(BTreeMap<String, Kind>),
	/// A boolean literal
	Bool(bool),
}

impl Eq for KindLiteral {}
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

impl hash::Hash for KindLiteral {
	fn hash<H: hash::Hasher>(&self, state: &mut H) {
		std::mem::discriminant(self).hash(state);
		match self {
			KindLiteral::String(strand) => strand.hash(state),
			KindLiteral::Integer(x) => x.hash(state),
			KindLiteral::Float(x) => x.to_bits().hash(state),
			KindLiteral::Decimal(decimal) => decimal.hash(state),
			KindLiteral::Duration(duration) => duration.hash(state),
			KindLiteral::Array(kinds) => kinds.hash(state),
			KindLiteral::Object(btree_map) => btree_map.hash(state),
			KindLiteral::Bool(x) => x.hash(state),
		}
	}
}
