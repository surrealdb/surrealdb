use std::fmt::{self, Write};

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::fmt::EscapeIdent;
use crate::sql::{Expr, Permission};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineModelStatement {
	pub kind: DefineKind,
	pub hash: String,
	pub name: String,
	pub version: String,
	pub comment: Option<Expr>,
	pub permissions: Permission,
}

impl Default for DefineModelStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			hash: String::new(),
			name: String::new(),
			version: String::new(),
			comment: None,
			permissions: Permission::default(),
		}
	}
}

impl ToSql for DefineModelStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "DEFINE MODEL");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, fmt, " IF NOT EXISTS"),
		}
		write_sql!(f, fmt, " ml::{}<{}>", EscapeIdent(&self.name), self.version);
		if let Some(comment) = self.comment.as_ref() {
			write_sql!(f, fmt, " COMMENT {}", comment);
		}
		write_sql!(f, fmt, " PERMISSIONS {}", self.permissions);
	}
}

impl From<DefineModelStatement> for crate::expr::statements::DefineModelStatement {
	fn from(v: DefineModelStatement) -> Self {
		Self {
			kind: v.kind.into(),
			hash: v.hash,
			name: v.name,
			version: v.version,
			comment: v.comment.map(|x| x.into()),
			permissions: v.permissions.into(),
		}
	}
}

impl From<crate::expr::statements::DefineModelStatement> for DefineModelStatement {
	fn from(v: crate::expr::statements::DefineModelStatement) -> Self {
		Self {
			kind: v.kind.into(),
			hash: v.hash,
			name: v.name,
			version: v.version,
			comment: v.comment.map(|x| x.into()),
			permissions: v.permissions.into(),
		}
	}
}
