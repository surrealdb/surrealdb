pub mod table {
	use crate::ctx::Context;
	use crate::dbs::Options;
	use crate::err::Error;
	use crate::expr::{Base, Value};
	use crate::iam::{Action, ResourceKind};
	use anyhow::Result;

	pub async fn exists(
		(ctx, opt): (&Context, Option<&Options>),
		(arg,): (String,),
	) -> Result<Value> {
		if let Some(opt) = opt {
			opt.valid_for_db()?;
			opt.is_allowed(Action::View, ResourceKind::Table, &Base::Db)?;
			let (ns, db) = opt.ns_db()?;
			let txn = ctx.tx();
			if let Err(err) = txn.get_tb(ns, db, arg.as_str()).await {
				// If error is table not found, return false,
				// otherwise propagate the error.
				if err.is::<Error>()
					&& err
						.downcast_ref::<Error>()
						.is_some_and(|e| matches!(e, Error::TbNotFound { .. }))
				{
					// Table does not exist
					Ok(Value::Bool(false))
				} else {
					// Some other error, propagate it
					Err(err)
				}
			} else {
				// Table exists
				Ok(Value::Bool(true))
			}
		} else {
			Ok(Value::None)
		}
	}
}
