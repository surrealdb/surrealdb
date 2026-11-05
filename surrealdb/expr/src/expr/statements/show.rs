use common::fmt::SqlDatetime;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::val::{Datetime, TableName};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ShowSince {
	Timestamp(Datetime),
	Versionstamp(u64),
}

/// A SHOW CHANGES statement for displaying changes made to a table or database.

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ShowStatement {
	pub table: Option<TableName>,
	pub since: ShowSince,
	pub limit: Option<u32>,
}

impl ToSql for ShowStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "SHOW CHANGES FOR");
		match self.table {
			Some(ref v) => write_sql!(f, fmt, " TABLE {}", common::fmt::EscapeIdent(v.as_str())),
			None => write_sql!(f, fmt, " DATABASE"),
		}
		match self.since {
			ShowSince::Timestamp(ref v) => write_sql!(f, fmt, " SINCE {}", SqlDatetime(v.0)),
			ShowSince::Versionstamp(ref v) => write_sql!(f, fmt, " SINCE {}", v),
		}
		if let Some(ref v) = self.limit {
			write_sql!(f, fmt, " LIMIT {}", v)
		}
	}
}
