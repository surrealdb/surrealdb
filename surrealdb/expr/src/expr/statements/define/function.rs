use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use super::DefineKind;
use crate::expr::permission::Permission;
use crate::expr::{Block, Expr, Kind};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineFunctionStatement {
	pub kind: DefineKind,
	pub name: Strand,
	pub args: Vec<(String, Kind)>,
	pub block: Block,
	pub comment: Expr,
	pub permissions: Permission,
	pub returns: Option<Kind>,
	pub graphql_alias: Option<String>,
	pub graphql_deprecated: Option<String>,
}

impl ToSql for DefineFunctionStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::define::DefineFunctionStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
