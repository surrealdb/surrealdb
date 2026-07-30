use anyhow::Result;

use crate::catalog::Error;
use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::expr::Base;
use crate::expr::statements::remove::function::RemoveFunctionStatement;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

/// Process this type returning a computed simple Value
pub(crate) async fn remove_function_statement_compute(
	this: &RemoveFunctionStatement,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Function, Base::Db)?;
	// Get the transaction
	let txn = ctx.tx();
	// Get the definition
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let fc = match txn.get_db_function(ns, db, &this.name, None).await {
		Ok(x) => x,
		Err(e) => {
			if this.if_exists && matches!(e.downcast_ref(), Some(Error::FcNotFound { .. })) {
				return Ok(Value::None);
			} else {
				return Err(e);
			}
		}
	};
	// Delete the definition
	let key = crate::key::database::fc::Fc {
		prefix: crate::key::database::all::DatabaseRoot {
			ns,
			db,
		},
		fc: std::borrow::Cow::Borrowed(&fc.name),
	};
	txn.del_key(&key).await?;
	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
