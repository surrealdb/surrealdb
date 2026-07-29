use chrono::{DateTime, Utc};
use common::fmt::{EscapeKwFreeIdent, SqlDatetime};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::TableName;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum ShowSince {
	Timestamp(DateTime<Utc>),
	Versionstamp(u64),
}

/// A SHOW CHANGES statement for displaying changes made to a table or database.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ShowStatement {
	pub table: Option<TableName>,
	pub since: ShowSince,
	pub limit: Option<u32>,
}

impl ToSql for ShowStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "SHOW CHANGES FOR");
		match self.table {
			Some(ref v) => write_sql!(f, fmt, " TABLE {}", EscapeKwFreeIdent(v.as_str())),
			None => write_sql!(f, fmt, " DATABASE"),
		}
		match self.since {
			ShowSince::Timestamp(ref v) => write_sql!(f, fmt, " SINCE {}", SqlDatetime(*v)),
			ShowSince::Versionstamp(ref v) => write_sql!(f, fmt, " SINCE {}", v),
		}
		if let Some(ref v) = self.limit {
			write_sql!(f, fmt, " LIMIT {}", v)
		}
	}
}
