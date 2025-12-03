use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::fmt::CoverStmts;
use crate::sql::{Expr, Literal, Permission};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefineBucketStatement {
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

impl From<DefineBucketStatement> for crate::expr::statements::define::DefineBucketStatement {
	fn from(v: DefineBucketStatement) -> Self {
		crate::expr::statements::define::DefineBucketStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			backend: v.backend.map(Into::into),
			permissions: v.permissions.into(),
			readonly: v.readonly,
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::define::DefineBucketStatement> for DefineBucketStatement {
	fn from(v: crate::expr::statements::define::DefineBucketStatement) -> Self {
		DefineBucketStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			backend: v.backend.map(Into::into),
			permissions: v.permissions.into(),
			readonly: v.readonly,
			comment: v.comment.into(),
		}
	}
}
