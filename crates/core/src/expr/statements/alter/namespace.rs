use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::Context;
use crate::dbs::Options;
use crate::val::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct AlterNamespaceStatement {
	pub compact: bool,
}

impl AlterNamespaceStatement {
	pub(crate) async fn compute(&self, _ctx: &Context, _opt: &Options) -> anyhow::Result<Value> {
		todo!()
	}
}

impl ToSql for AlterNamespaceStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterNamespaceStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
