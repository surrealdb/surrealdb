use surrealdb_types::{SqlFormat, ToSql};

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
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterNamespaceStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
