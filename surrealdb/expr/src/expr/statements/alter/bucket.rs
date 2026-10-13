use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::expr::permission::Permission;
use crate::expr::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AlterBucketStatement {
	pub name: Expr,
	pub if_exists: bool,
	pub backend: AlterKind<String>,
	pub permissions: Option<Permission>,
	pub readonly: AlterKind<()>,
	pub comment: AlterKind<String>,
}

impl Default for AlterBucketStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
			backend: AlterKind::None,
			permissions: None,
			readonly: AlterKind::None,
			comment: AlterKind::None,
		}
	}
}

impl ToSql for AlterBucketStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterBucketStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
