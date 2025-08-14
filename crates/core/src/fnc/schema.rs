pub mod table {
	use anyhow::Result;

	use crate::ctx::Context;
	use crate::dbs::Options;
	use crate::expr::Base;
	use crate::iam::{Action, ResourceKind};
	use crate::val::Value;

	pub async fn exists(
		(ctx, opt): (&Context, Option<&Options>),
		(arg,): (String,),
	) -> Result<Value> {
		if let Some(opt) = opt {
			opt.valid_for_db()?;
			opt.is_allowed(Action::View, ResourceKind::Table, &Base::Db)?;
			let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
			let txn = ctx.tx();
			let table_exists = txn.get_tb(ns, db, arg.as_str()).await?.is_some();
			Ok(Value::Bool(table_exists))
		} else {
			Ok(Value::None)
		}
	}
}
