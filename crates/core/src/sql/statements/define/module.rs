use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::sql::{Expr, ModuleExecutable, Permission};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefineModuleStatement {
	pub kind: DefineKind,
	pub name: Option<String>,
	pub executable: ModuleExecutable,
	pub comment: Option<Expr>,
	pub permissions: Permission,
}

impl ToSql for DefineModuleStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		f.push_str("DEFINE MODULE");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => f.push_str(" OVERWRITE"),
			DefineKind::IfNotExists => f.push_str(" IF NOT EXISTS"),
		}
		if let Some(name) = &self.name {
			write_sql!(f, sql_fmt, " mod::{} AS", name);
		}
		write_sql!(f, sql_fmt, " {}", self.executable);
		if let Some(ref v) = self.comment {
			write_sql!(f, sql_fmt, " COMMENT {}", v);
		}
		if sql_fmt.is_pretty() {
			f.push('\n');
			sql_fmt.write_indent(f);
		} else {
			f.push(' ');
		}
		write_sql!(f, sql_fmt, "PERMISSIONS {}", self.permissions);
	}
}

impl From<DefineModuleStatement> for crate::expr::statements::DefineModuleStatement {
	fn from(v: DefineModuleStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name,
			executable: v.executable.into(),
			comment: v.comment.map(|x| x.into()),
			permissions: v.permissions.into(),
		}
	}
}

impl From<crate::expr::statements::DefineModuleStatement> for DefineModuleStatement {
	fn from(v: crate::expr::statements::DefineModuleStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name,
			executable: v.executable.into(),
			comment: v.comment.map(|x| x.into()),
			permissions: v.permissions.into(),
		}
	}
}
