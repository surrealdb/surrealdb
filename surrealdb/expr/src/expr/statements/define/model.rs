use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::expr::permission::Permission;
use crate::expr::{CoverStmts, Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineModelStatement {
	pub kind: DefineKind,
	pub hash: Strand,
	pub name: Strand,
	pub version: Strand,
	pub comment: Expr,
	pub permissions: Permission,
}

impl ToSql for DefineModelStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "DEFINE MODEL");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, fmt, " IF NOT EXISTS"),
		}
		write_sql!(
			f,
			fmt,
			" ml::{}<{}>",
			common::fmt::EscapeKwFreeIdent(self.name.as_str()),
			self.version.as_str()
		);
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
		write_sql!(f, fmt, " PERMISSIONS {}", self.permissions);
	}
}
