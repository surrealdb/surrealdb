use crate::ctx::Context;
use crate::dbs::Options;
use crate::val::Value;
use surrealdb_types::{SqlFormat, ToSql};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct AlterDatabaseStatement {
	pub compact: bool,
}

impl AlterDatabaseStatement {
	pub(crate) async fn compute(&self, _ctx: &Context, _opt: &Options) -> anyhow::Result<Value> {
		todo!()
	}
}

impl ToSql for AlterDatabaseStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterDatabaseStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
