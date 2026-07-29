use common::fmt::SqlDuration;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
pub struct SleepStatement {
	pub duration: std::time::Duration,
}

impl ToSql for SleepStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "SLEEP {}", SqlDuration(self.duration));
	}
}
