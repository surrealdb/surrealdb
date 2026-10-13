use surrealdb_types::{SqlFormat, ToSql, write_sql};

pub mod key;
pub use key::{RecordIdKeyGen, RecordIdKeyLit};
pub mod range;
pub use range::RecordIdKeyRangeLit;

use crate::TableName;

/// A record id literal, needs to be evaluated to get the actual record id.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RecordIdLit {
	/// Table name
	pub table: TableName,
	pub key: RecordIdKeyLit,
}

impl ToSql for RecordIdLit {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "{}:{}", self.table, self.key);
	}
}
