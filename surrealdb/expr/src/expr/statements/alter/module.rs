use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::expr::module::ModuleName;
use crate::expr::permission::Permission;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AlterModuleStatement {
	pub name: ModuleName,
	pub if_exists: bool,
	pub comment: AlterKind<String>,
	pub permissions: Option<Permission>,
}

impl ToSql for AlterModuleStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterModuleStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
