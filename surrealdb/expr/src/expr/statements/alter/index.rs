use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::statements::alter::AlterKind;
use crate::expr::{CoverStmts, Expr, Literal};

/// Represents an `ALTER INDEX` statement.
///
/// Currently supports decommissioning indexes as a safe preparation step before removal.
/// Decommissioning an index:
/// - Cancels any ongoing concurrent index builds
/// - Prevents the query planner from using the index
/// - Stops updating the index on record changes
///
/// This allows administrators to verify query performance before permanently removing an index.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AlterIndexStatement {
	pub name: Expr,
	pub table: Expr,
	pub if_exists: bool,
	/// If true, marks the index as decommissioned
	pub prepare_remove: bool,
	pub comment: AlterKind<String>,
}

impl Default for AlterIndexStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			table: Expr::Literal(Literal::None),
			if_exists: false,
			prepare_remove: false,
			comment: AlterKind::None,
		}
	}
}

impl ToSql for AlterIndexStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER INDEX");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {} ON {}", CoverStmts(&self.name), CoverStmts(&self.table));

		if self.prepare_remove {
			write_sql!(f, fmt, " PREPARE REMOVE");
		}
		match self.comment {
			AlterKind::Set(ref x) => {
				use common::fmt::QuoteStr;
				write_sql!(f, fmt, " COMMENT {}", QuoteStr(x))
			}
			AlterKind::Drop => write_sql!(f, fmt, " DROP COMMENT"),
			AlterKind::None => {}
		}
	}
}
