use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::Expr;
use crate::statements::alter::AlterKind;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
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
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER SYSTEM");
		match &self.query_timeout {
			AlterKind::None => {}
			AlterKind::Set(duration) => {
				write_sql!(f, fmt, " QUERY_TIMEOUT {}", duration);
			}
			AlterKind::Drop => {
				write_sql!(f, fmt, " DROP QUERY_TIMEOUT");
			}
		}
		if self.compact {
			write_sql!(f, fmt, " COMPACT");
		}
	}
}
