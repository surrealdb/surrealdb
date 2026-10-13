use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::{CoverStmts, Expr, Literal, Permission};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineBucketStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub backend: Option<Expr>,
	pub permissions: Permission,
	pub readonly: bool,
	pub comment: Expr,
}

impl Default for DefineBucketStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			backend: None,
			permissions: Permission::default(),
			readonly: false,
			comment: Expr::Literal(Literal::None),
		}
	}
}

impl ToSql for DefineBucketStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "DEFINE BUCKET");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, sql_fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, sql_fmt, " IF NOT EXISTS"),
		}
		write_sql!(f, sql_fmt, " {}", CoverStmts(&self.name));

		if self.readonly {
			write_sql!(f, sql_fmt, " READONLY");
		}

		if let Some(ref backend) = self.backend {
			write_sql!(f, sql_fmt, " BACKEND {}", CoverStmts(backend));
		}

		write_sql!(f, sql_fmt, " PERMISSIONS {}", self.permissions);

		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, sql_fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
	}
}
