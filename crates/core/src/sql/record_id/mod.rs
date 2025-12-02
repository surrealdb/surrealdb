use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::EscapeRid;

pub mod key;
pub(crate) use key::{RecordIdKeyGen, RecordIdKeyLit};
pub mod range;
pub use range::RecordIdKeyRangeLit;

/// A record id literal, needs to be evaluated to get the actual record id.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct RecordIdLit {
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

impl ToSql for RecordIdLit {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "{}:{}", EscapeRid(&self.table), self.key);
	}
}
