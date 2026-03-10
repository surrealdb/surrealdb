use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::fmt::{CoverStmts, EscapeKwFreeIdent};
use crate::sql::{Expr, Literal, Permission};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefineParamStatement {
	pub kind: DefineKind,
	pub name: String,
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
		write_sql!(f, fmt, " ${} VALUE {}", EscapeKwFreeIdent(&self.name), CoverStmts(&self.value));
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
		let fmt = fmt.increment();
		write_sql!(f, fmt, " PERMISSIONS {}", self.permissions);
	}
}

impl From<DefineParamStatement> for crate::expr::statements::DefineParamStatement {
	fn from(v: DefineParamStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name,
			value: v.value.into(),
			comment: v.comment.into(),
			permissions: v.permissions.into(),
		}
	}
}

impl From<crate::expr::statements::DefineParamStatement> for DefineParamStatement {
	fn from(v: crate::expr::statements::DefineParamStatement) -> Self {
		DefineParamStatement {
			kind: v.kind.into(),
			name: v.name,
			value: v.value.into(),
			comment: v.comment.into(),
			permissions: v.permissions.into(),
		}
	}
}
