use surrealdb_types::{SqlFormat, ToSql, write_sql};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
/// Executes `ALTER NAMESPACE` operations for the current namespace.
///
/// Supported options:
/// - `compact`: triggers a compaction of the current namespace keyspace.
pub struct AlterNamespaceStatement {
	/// When true, compacts the underlying storage for the namespace.
	pub compact: bool,
}

impl ToSql for AlterNamespaceStatement {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		write_sql!(f, _fmt, "ALTER NAMESPACE");
		if self.compact {
			write_sql!(f, _fmt, " COMPACT");
		}
	}
}
