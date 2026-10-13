use common::fmt::QuoteStr;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::{CoverStmts, Expr, Literal, Permission};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// AST node for `ALTER BUCKET`.
pub struct AlterBucketStatement {
	pub name: Expr,
	pub if_exists: bool,
	pub backend: AlterKind<String>,
	pub permissions: Option<Permission>,
	pub readonly: AlterKind<()>,
	pub comment: AlterKind<String>,
}

impl Default for AlterBucketStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
			backend: AlterKind::None,
			permissions: None,
			readonly: AlterKind::None,
			comment: AlterKind::None,
		}
	}
}

impl ToSql for AlterBucketStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER BUCKET");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {}", CoverStmts(&self.name));

		match self.readonly {
			AlterKind::Set(_) => write_sql!(f, fmt, " READONLY"),
			AlterKind::Drop => write_sql!(f, fmt, " DROP READONLY"),
			AlterKind::None => {}
		}

		match self.backend {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " BACKEND {}", QuoteStr(v)),
			AlterKind::Drop => f.push_str(" DROP BACKEND"),
			AlterKind::None => {}
		}

		if let Some(ref p) = self.permissions {
			write_sql!(f, fmt, " PERMISSIONS {}", p);
		}

		match self.comment {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " COMMENT {}", QuoteStr(v)),
			AlterKind::Drop => f.push_str(" DROP COMMENT"),
			AlterKind::None => {}
		}
	}
}
