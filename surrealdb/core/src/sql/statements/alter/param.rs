use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::fmt::{CoverStmts, EscapeKwFreeIdent, QuoteStr};
use crate::sql::{Expr, Permission};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// AST node for `ALTER PARAM`.
pub struct AlterParamStatement {
	pub name: String,
	pub if_exists: bool,
	pub value: Option<Expr>,
	pub comment: AlterKind<String>,
	pub permissions: Option<Permission>,
}

impl ToSql for AlterParamStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER PARAM");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " ${}", EscapeKwFreeIdent(&self.name));

		if let Some(ref v) = self.value {
			write_sql!(f, fmt, " VALUE {}", CoverStmts(v));
		}

		match self.comment {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " COMMENT {}", QuoteStr(v)),
			AlterKind::Drop => f.push_str(" DROP COMMENT"),
			AlterKind::None => {}
		}

		if let Some(ref p) = self.permissions {
			let fmt = fmt.increment();
			write_sql!(f, fmt, " PERMISSIONS {}", p);
		}
	}
}

impl From<AlterParamStatement> for crate::expr::statements::alter::AlterParamStatement {
	fn from(v: AlterParamStatement) -> Self {
		crate::expr::statements::alter::AlterParamStatement {
			name: v.name,
			if_exists: v.if_exists,
			value: v.value.map(Into::into),
			comment: v.comment.into(),
			permissions: v.permissions.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::alter::AlterParamStatement> for AlterParamStatement {
	fn from(v: crate::expr::statements::alter::AlterParamStatement) -> Self {
		AlterParamStatement {
			name: v.name,
			if_exists: v.if_exists,
			value: v.value.map(Into::into),
			comment: v.comment.into(),
			permissions: v.permissions.map(Into::into),
		}
	}
}
