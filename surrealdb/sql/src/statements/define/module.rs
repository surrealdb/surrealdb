use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::{CoverStmts, Expr, Ident, Literal, ModuleExecutable, Permission};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineModuleStatement {
	pub kind: DefineKind,
	pub name: Option<Ident>,
	pub executable: ModuleExecutable,
	pub comment: Expr,
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
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, sql_fmt, " COMMENT {}", CoverStmts(&self.comment));
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
