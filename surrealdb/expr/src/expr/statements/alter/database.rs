use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::{CoverStmts, Expr};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
/// Executes `ALTER DATABASE` operations on the database `name` names in the
/// session's namespace, or on the session's database when it names none.
///
/// Supported options:
/// - `compact`: triggers a compaction of the database's keyspace.
/// - `compatibility`: sets the database's compatibility level. Raising it to 4 queues a
///   verification of the stored rows of every `SCHEMAFULL` table.
pub struct AlterDatabaseStatement {
	pub name: Option<Expr>,
	pub compact: bool,
	pub compatibility: Option<u8>,
}

impl ToSql for AlterDatabaseStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER DATABASE");
		if let Some(name) = &self.name {
			write_sql!(f, fmt, " {}", CoverStmts(name));
		}
		if self.compact {
			write_sql!(f, fmt, " COMPACT");
		}
		if let Some(level) = self.compatibility {
			write_sql!(f, fmt, " COMPATIBILITY {level}");
		}
	}
}
