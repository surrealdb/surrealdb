use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::Expr;
use crate::sql::statements::alter::AlterKind;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// AST node for `ALTER SYSTEM`.
///
/// Supported operations:
/// - `QUERY_TIMEOUT <duration>`: sets the global query timeout
/// - `DROP QUERY_TIMEOUT`: clears the global query timeout
/// - `COMPACT`: requests datastore‑wide compaction
pub struct AlterSystemStatement {
	/// Tri‑state alteration for the `QUERY_TIMEOUT` parameter.
	pub query_timeout: AlterKind<Expr>,
	/// When true, emits `COMPACT`.
	pub compact: bool,
}

impl ToSql for AlterSystemStatement {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		write_sql!(f, _fmt, "ALTER SYSTEM");
		match &self.query_timeout {
			AlterKind::None => {}
			AlterKind::Set(duration) => {
				write_sql!(f, _fmt, " QUERY_TIMEOUT {}", duration);
			}
			AlterKind::Drop => {
				write_sql!(f, _fmt, " DROP QUERY_TIMEOUT");
			}
		}
		if self.compact {
			write_sql!(f, _fmt, " COMPACT");
		}
	}
}

impl From<AlterSystemStatement> for crate::expr::statements::alter::AlterSystemStatement {
	fn from(v: AlterSystemStatement) -> Self {
		crate::expr::statements::alter::AlterSystemStatement {
			query_timeout: v.query_timeout.into(),
			compact: v.compact,
		}
	}
}
impl From<crate::expr::statements::alter::AlterSystemStatement> for AlterSystemStatement {
	fn from(v: crate::expr::statements::alter::AlterSystemStatement) -> Self {
		AlterSystemStatement {
			query_timeout: v.query_timeout.into(),
			compact: v.compact,
		}
	}
}
