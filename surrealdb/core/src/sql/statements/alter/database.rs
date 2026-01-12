use surrealdb_types::{SqlFormat, ToSql, write_sql};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// AST node for `ALTER DATABASE`.
///
/// Currently supports the `COMPACT` maintenance operation, which instructs the
/// underlying datastore to compact the current database keyspace.
pub struct AlterDatabaseStatement {
	pub compact: bool,
}

impl ToSql for AlterDatabaseStatement {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		write_sql!(f, _fmt, "ALTER DATABASE");
		if self.compact {
			write_sql!(f, _fmt, " COMPACT");
		}
	}
}

impl From<AlterDatabaseStatement> for crate::expr::statements::alter::AlterDatabaseStatement {
	fn from(v: AlterDatabaseStatement) -> Self {
		crate::expr::statements::alter::AlterDatabaseStatement {
			compact: v.compact,
		}
	}
}
impl From<crate::expr::statements::alter::AlterDatabaseStatement> for AlterDatabaseStatement {
	fn from(v: crate::expr::statements::alter::AlterDatabaseStatement) -> Self {
		AlterDatabaseStatement {
			compact: v.compact,
		}
	}
}
