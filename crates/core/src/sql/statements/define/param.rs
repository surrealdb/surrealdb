use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::fmt::{EscapeIdent, is_pretty, pretty_indent};
use crate::sql::{Expr, Permission};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefineParamStatement {
	pub kind: DefineKind,
	pub name: String,
	pub value: Expr,
	pub comment: Option<Expr>,
	pub permissions: Permission,
}

impl ToSql for DefineParamStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "DEFINE PARAM");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, sql_fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, sql_fmt, " IF NOT EXISTS"),
		}
		write_sql!(f, sql_fmt, " ${} VALUE {}", EscapeIdent(&self.name), self.value);
		if let Some(ref v) = self.comment {
			write_sql!(f, sql_fmt, " COMMENT {}", v);
		}
		if is_pretty() {
			let _indent = pretty_indent();
		} else {
			f.push(' ');
		}
		write_sql!(f, sql_fmt, "PERMISSIONS {}", self.permissions);
	}
}

impl From<DefineParamStatement> for crate::expr::statements::DefineParamStatement {
	fn from(v: DefineParamStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name,
			value: v.value.into(),
			comment: v.comment.map(|x| x.into()),
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
			comment: v.comment.map(|x| x.into()),
			permissions: v.permissions.into(),
		}
	}
}
