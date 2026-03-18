use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::fmt::{EscapeKwFreeIdent, QuoteStr};
use crate::sql::Permission;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// AST node for `ALTER BUCKET`.
pub struct AlterBucketStatement {
	pub name: String,
	pub if_exists: bool,
	pub backend: AlterKind<String>,
	pub permissions: Option<Permission>,
	pub readonly: AlterKind<()>,
	pub comment: AlterKind<String>,
}

impl ToSql for AlterBucketStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER BUCKET");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {}", EscapeKwFreeIdent(&self.name));

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

impl From<AlterBucketStatement> for crate::expr::statements::alter::AlterBucketStatement {
	fn from(v: AlterBucketStatement) -> Self {
		crate::expr::statements::alter::AlterBucketStatement {
			name: v.name,
			if_exists: v.if_exists,
			backend: v.backend.into(),
			permissions: v.permissions.map(Into::into),
			readonly: v.readonly.into(),
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::alter::AlterBucketStatement> for AlterBucketStatement {
	fn from(v: crate::expr::statements::alter::AlterBucketStatement) -> Self {
		AlterBucketStatement {
			name: v.name,
			if_exists: v.if_exists,
			backend: v.backend.into(),
			permissions: v.permissions.map(Into::into),
			readonly: v.readonly.into(),
			comment: v.comment.into(),
		}
	}
}
