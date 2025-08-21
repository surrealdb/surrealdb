use std::fmt;

use crate::sql::escape::EscapeRid;

pub mod key;
pub use key::{RecordIdKeyGen, RecordIdKeyLit};
pub mod range;
pub use range::RecordIdKeyRangeLit;

/// A record id literal, needs to be evaluated to get the actual record id.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RecordIdLit {
	/// Table name
	pub table: String,
	pub key: RecordIdKeyLit,
}

impl From<RecordIdLit> for crate::expr::RecordIdLit {
	fn from(v: RecordIdLit) -> Self {
		crate::expr::RecordIdLit {
			table: v.table,
			key: v.key.into(),
		}
	}
}

impl From<crate::expr::RecordIdLit> for RecordIdLit {
	fn from(v: crate::expr::RecordIdLit) -> Self {
		RecordIdLit {
			table: v.table,
			key: v.key.into(),
		}
	}
}

impl fmt::Display for RecordIdLit {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}:{}", EscapeRid(&self.table), self.key)
	}
}
