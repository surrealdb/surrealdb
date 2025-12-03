use std::collections::BTreeMap;
use std::hash;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::sql::fmt_sql_comma_separated;
use crate::utils::escape::QuoteStr;
use crate::{Duration, Kind, SqlFormat, ToSql, Value};

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

impl KindLiteral {
	/// Check if a value matches this literal
	pub fn matches(&self, value: &Value) -> bool {
		match self {
			KindLiteral::String(s) => {
				if let Value::String(v) = value {
					s == v
				} else {
					false
				}
			}
			KindLiteral::Integer(i) => {
				if let Value::Number(crate::Number::Int(v)) = value {
					i == v
				} else {
					false
				}
			}
			KindLiteral::Float(f) => {
				if let Value::Number(crate::Number::Float(v)) = value {
					f.to_bits() == v.to_bits()
				} else {
					false
				}
			}
			KindLiteral::Decimal(d) => {
				if let Value::Number(crate::Number::Decimal(v)) = value {
					d == v
				} else {
					false
				}
			}
			KindLiteral::Duration(d) => {
				if let Value::Duration(v) = value {
					d == v
				} else {
					false
				}
			}
			KindLiteral::Array(kinds) => {
				if let Value::Array(arr) = value {
					if kinds.len() != arr.len() {
						return false;
					}
					kinds.iter().zip(arr.iter()).all(|(kind, val)| val.is_kind(kind))
				} else {
					false
				}
			}
			KindLiteral::Object(kinds) => {
				if let Value::Object(obj) = value {
					if kinds.len() != obj.len() {
						return false;
					}
					kinds.iter().all(|(key, kind)| {
						obj.get(key).map(|val| val.is_kind(kind)).unwrap_or(false)
					})
				} else {
					false
				}
			}
			KindLiteral::Bool(b) => {
				if let Value::Bool(v) = value {
					b == v
				} else {
					false
				}
			}
		}
	}
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

impl ToSql for KindLiteral {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			KindLiteral::String(string) => QuoteStr(string).fmt_sql(f, fmt),
			KindLiteral::Integer(x) => x.fmt_sql(f, fmt),
			KindLiteral::Float(v) => {
				if v.is_finite() {
					// Add suffix to distinguish between int and float
					v.fmt_sql(f, fmt);
					f.push('f');
				} else {
					// Don't add suffix for NaN, inf, -inf
					v.fmt_sql(f, fmt);
				}
			}
			KindLiteral::Decimal(v) => {
				v.fmt_sql(f, fmt);
				f.push_str("dec");
			}
			KindLiteral::Duration(duration) => duration.fmt_sql_internal(f),
			KindLiteral::Array(kinds) => {
				f.push('[');
				fmt_sql_comma_separated(kinds, f, fmt);
				f.push(']');
			}
			KindLiteral::Object(btree_map) => {
				f.push('{');
				let items = btree_map
					.iter()
					.map(|(k, v)| format!("{}: {}", k.to_sql(), v.to_sql()))
					.collect::<Vec<String>>();
				fmt_sql_comma_separated(&items, f, fmt);
				f.push('}');
			}
			KindLiteral::Bool(x) => x.fmt_sql(f, fmt),
		}
	}
}
