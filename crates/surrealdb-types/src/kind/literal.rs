use std::{collections::BTreeMap, hash};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{Duration, Kind};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum KindLiteral {
	String(String),
	Integer(i64),
	Float(f64),
	Decimal(Decimal),
	Duration(Duration),
	Array(Vec<Kind>),
	Object(BTreeMap<String, Kind>),
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