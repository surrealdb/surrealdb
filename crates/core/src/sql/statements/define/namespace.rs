use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::fmt::CoverStmts;
use crate::sql::{Expr, Literal};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineNamespaceStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: Expr,
	pub comment: Expr,
}

impl Default for DefineNamespaceStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			id: None,
			name: Expr::Literal(Literal::None),
			comment: Expr::Literal(Literal::None),
		}
	}
}

impl ToSql for DefineNamespaceStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "DEFINE NAMESPACE");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, sql_fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, sql_fmt, " IF NOT EXISTS"),
		}
		write_sql!(f, sql_fmt, " {}", CoverStmts(&self.name));
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, sql_fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
	}
}

impl From<DefineNamespaceStatement> for crate::expr::statements::DefineNamespaceStatement {
	fn from(v: DefineNamespaceStatement) -> Self {
		Self {
			kind: v.kind.into(),
			id: v.id,
			name: v.name.into(),
			comment: v.comment.into(),
		}
	}
}

#[allow(clippy::fallible_impl_from)]
impl From<crate::expr::statements::DefineNamespaceStatement> for DefineNamespaceStatement {
	fn from(v: crate::expr::statements::DefineNamespaceStatement) -> Self {
		Self {
			kind: v.kind.into(),
			id: v.id,
			name: v.name.into(),
			comment: v.comment.into(),
		}
	}
}
