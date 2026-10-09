use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::expr::permission::Permission;
use crate::expr::{CoverStmts, Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineParamStatement {
	pub kind: DefineKind,
	pub name: Strand,
	pub value: Expr,
	pub comment: Expr,
	pub permissions: Permission,
}

impl ToSql for DefineParamStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "DEFINE PARAM");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, fmt, " IF NOT EXISTS"),
		}
		write_sql!(
			f,
			fmt,
			" ${} VALUE {}",
			common::fmt::EscapeKwFreeIdent(self.name.as_str()),
			CoverStmts(&self.value)
		);
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
		let fmt = fmt.increment();
		write_sql!(f, fmt, " PERMISSIONS {}", self.permissions);
	}
}
