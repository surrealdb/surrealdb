use surrealdb_types::{SqlFormat, ToSql};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
/// Executes `ALTER DATABASE` operations.
///
/// Supported options:
/// - `compact`: triggers a compaction of the current database keyspace.
pub struct AlterDatabaseStatement {
	pub compact: bool,
}

impl ToSql for AlterDatabaseStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterDatabaseStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
