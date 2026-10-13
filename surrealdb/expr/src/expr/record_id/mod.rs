use common::fmt::EscapeIdent;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::val::TableName;

pub mod key;
pub use key::{RecordIdKeyGen, RecordIdKeyLit};
pub mod range;
pub use range::RecordIdKeyRangeLit;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RecordIdLit {
	/// Table name
	pub table: TableName,
	pub key: RecordIdKeyLit,
}

impl RecordIdLit {
	pub fn is_static(&self) -> bool {
		self.key.is_static()
	}

	/// Whether evaluating this record id can modify data; see
	/// [`Literal::read_only`](crate::expr::Literal::read_only).
	pub fn read_only(&self) -> bool {
		self.key.read_only()
	}
}

impl ToSql for RecordIdLit {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "{}:{}", EscapeIdent(&self.table), self.key)
	}
}
