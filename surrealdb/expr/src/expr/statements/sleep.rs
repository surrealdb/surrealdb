use common::fmt::SqlDuration;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::val::Duration;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct SleepStatement {
	pub duration: Duration,
}

impl ToSql for SleepStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "SLEEP {}", SqlDuration(self.duration.0));
	}
}
