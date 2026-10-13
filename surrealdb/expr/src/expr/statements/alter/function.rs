use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::expr::permission::Permission;
use crate::expr::{Block, Kind};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct AlterFunctionStatement {
	pub name: Strand,
	pub if_exists: bool,
	pub args: AlterKind<Vec<(String, Kind)>>,
	pub block: AlterKind<Block>,
	pub comment: AlterKind<String>,
	pub permissions: Option<Permission>,
	pub returns: AlterKind<Kind>,
}

impl ToSql for AlterFunctionStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterFunctionStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
