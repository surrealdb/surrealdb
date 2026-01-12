use surrealdb_types::{SqlFormat, ToSql, write_sql};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// AST node for `ALTER NAMESPACE`.
///
/// Currently supports the `COMPACT` maintenance operation, which instructs the
/// underlying datastore to compact the current namespace keyspace.
pub struct AlterNamespaceStatement {
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

impl From<AlterNamespaceStatement> for crate::expr::statements::alter::AlterNamespaceStatement {
	fn from(v: AlterNamespaceStatement) -> Self {
		crate::expr::statements::alter::AlterNamespaceStatement {
			compact: v.compact,
		}
	}
}
impl From<crate::expr::statements::alter::AlterNamespaceStatement> for AlterNamespaceStatement {
	fn from(v: crate::expr::statements::alter::AlterNamespaceStatement) -> Self {
		AlterNamespaceStatement {
			compact: v.compact,
		}
	}
}
