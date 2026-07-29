#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ChangeFeed {
	pub expiry: std::time::Duration,
	pub store_diff: bool,
}

impl surrealdb_types::ToSql for ChangeFeed {
	fn fmt_sql(&self, f: &mut String, sql_fmt: surrealdb_types::SqlFormat) {
		use surrealdb_types::write_sql;
		write_sql!(f, sql_fmt, "CHANGEFEED {}", common::fmt::SqlDuration(self.expiry));
		if self.store_diff {
			f.push_str(" INCLUDE ORIGINAL");
		}
	}
}
