use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::{CoverStmts, Expr, Ident, Literal, Permission};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineParamStatement {
	pub kind: DefineKind,
	pub name: Ident,
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
		write_sql!(f, fmt, " ${} VALUE {}", self.name, CoverStmts(&self.value));
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
		let fmt = fmt.increment();
		write_sql!(f, fmt, " PERMISSIONS {}", self.permissions);
	}
}
