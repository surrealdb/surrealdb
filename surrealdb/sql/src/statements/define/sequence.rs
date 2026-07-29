use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::{CoverStmts, Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineSequenceStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub batch: Expr,
	pub start: Expr,
	pub timeout: Expr,
}

impl Default for DefineSequenceStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			batch: Expr::Literal(Literal::Integer(0)),
			start: Expr::Literal(Literal::Integer(0)),
			timeout: Expr::Literal(Literal::None),
		}
	}
}

impl ToSql for DefineSequenceStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "DEFINE SEQUENCE");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, sql_fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, sql_fmt, " IF NOT EXISTS"),
		}
		write_sql!(
			f,
			sql_fmt,
			" {} BATCH {} START {}",
			CoverStmts(&self.name),
			CoverStmts(&self.batch),
			CoverStmts(&self.start)
		);
		if !matches!(self.timeout, Expr::Literal(Literal::None)) {
			write_sql!(f, sql_fmt, " TIMEOUT {}", CoverStmts(&self.timeout));
		}
	}
}
