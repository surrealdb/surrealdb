use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use super::DefineKind;
use crate::expr::Expr;
use crate::expr::permission::Permission;

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
		let stmt: crate::sql::statements::define::DefineModelStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
