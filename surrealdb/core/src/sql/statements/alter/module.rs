use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::fmt::QuoteStr;
use crate::sql::{ModuleName, Permission};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct AlterModuleStatement {
	pub name: ModuleName,
	pub if_exists: bool,
	pub comment: AlterKind<String>,
	pub permissions: Option<Permission>,
}

impl ToSql for AlterModuleStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER MODULE");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {}", self.name);

		match self.comment {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " COMMENT {}", QuoteStr(v)),
			AlterKind::Drop => f.push_str(" DROP COMMENT"),
			AlterKind::None => {}
		}

		if let Some(ref p) = self.permissions {
			write_sql!(f, fmt, " PERMISSIONS {}", p);
		}
	}
}

impl From<AlterModuleStatement> for crate::expr::statements::alter::AlterModuleStatement {
	fn from(v: AlterModuleStatement) -> Self {
		crate::expr::statements::alter::AlterModuleStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			comment: v.comment.into(),
			permissions: v.permissions.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::alter::AlterModuleStatement> for AlterModuleStatement {
	fn from(v: crate::expr::statements::alter::AlterModuleStatement) -> Self {
		AlterModuleStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			comment: v.comment.into(),
			permissions: v.permissions.map(Into::into),
		}
	}
}
