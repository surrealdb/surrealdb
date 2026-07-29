use common::fmt::Fmt;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::{CoverStmts, Expr, Index, Literal};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DefineIndexStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub what: Expr,
	pub cols: Vec<Expr>,
	pub index: Index,
	pub comment: Expr,
	pub concurrently: bool,
}

impl ToSql for DefineIndexStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "DEFINE INDEX");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, sql_fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, sql_fmt, " IF NOT EXISTS"),
		}
		write_sql!(f, sql_fmt, " {} ON {}", CoverStmts(&self.name), CoverStmts(&self.what));
		if !self.cols.is_empty() {
			write_sql!(
				f,
				sql_fmt,
				" FIELDS {}",
				Fmt::comma_separated(self.cols.iter().map(CoverStmts))
			);
		}
		if Index::Idx != self.index {
			write_sql!(f, sql_fmt, " {}", self.index);
		}
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, sql_fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
		if self.concurrently {
			write_sql!(f, sql_fmt, " CONCURRENTLY");
		}
	}
}
