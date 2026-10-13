use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::expr::Expr;
use crate::expr::permission::Permission;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct AlterParamStatement {
	pub name: Strand,
	pub if_exists: bool,
	pub value: Option<Expr>,
	pub comment: AlterKind<String>,
	pub permissions: Option<Permission>,
}

impl ToSql for AlterParamStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterParamStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
