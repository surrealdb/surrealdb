use crate::sql::{escape::EscapeRid, id::RecordIdKeyLit};
use std::fmt;

/// A record id literal, needs to be evaluated to get the actual record id.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RecordIdLit {
	/// Table name
	pub tb: String,
	pub id: RecordIdKeyLit,
}

impl From<RecordIdLit> for crate::expr::RecordIdLit {
	fn from(v: RecordIdLit) -> Self {
		crate::expr::RecordIdLit {
			tb: v.tb,
			id: v.id.into(),
		}
	}
}

impl From<crate::expr::RecordIdLit> for RecordIdLit {
	fn from(v: crate::expr::RecordIdLit) -> Self {
		RecordIdLit {
			tb: v.tb,
			id: v.id.into(),
		}
	}
}

impl fmt::Display for RecordIdLit {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}:{}", EscapeRid(&self.tb), self.id)
	}
}
