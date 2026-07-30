use anyhow::Result;

use crate::catalog::Error;
use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::expr::Base;
use crate::expr::statements::remove::param::RemoveParamStatement;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

/// Process this type returning a computed simple Value
pub(crate) async fn remove_param_statement_compute(
	this: &RemoveParamStatement,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Parameter, Base::Db)?;
	// Get the transaction
	let txn = ctx.tx();
	// Get the definition
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let pa = match txn.get_db_param(ns, db, &this.name, None).await {
		Ok(x) => x,
		Err(e) => {
			if this.if_exists && matches!(e.downcast_ref(), Some(Error::PaNotFound { .. })) {
				return Ok(Value::None);
			} else {
				return Err(e);
			}
		}
	};
	// Delete the definition
	let key = crate::key::database::pa::Pa {
		prefix: crate::key::database::all::DatabaseRoot {
			ns,
			db,
		},
		pa: std::borrow::Cow::Borrowed(&pa.name),
	};
	txn.del_key(&key).await?;
	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
