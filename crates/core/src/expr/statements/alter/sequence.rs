use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::providers::DatabaseProvider;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Base, Timeout, Value};
use crate::iam::{Action, ResourceKind};
use crate::key::database::sq::Sq;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct AlterSequenceStatement {
	pub name: String,
	pub if_exists: bool,
	pub timeout: Option<Timeout>,
}

impl AlterSequenceStatement {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Sequence, &Base::Db)?;
		// Get the NS and DB
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Get the sequence definition
		let mut sq = match txn.get_db_sequence(ns, db, &self.name).await {
			Ok(tb) => tb.deref().clone(),
			Err(e) => {
				if self.if_exists && matches!(e.downcast_ref(), Some(Error::SeqNotFound { .. })) {
					return Ok(Value::None);
				} else {
					return Err(e);
				}
			}
		};
		// Process the statement
		if let Some(timeout) = &self.timeout {
			let timeout = timeout.compute(stk, ctx, opt, doc).await?.0;
			if timeout.is_zero() {
				sq.timeout = None;
			} else {
				sq.timeout = Some(timeout);
			}
		}
		// Set the table definition
		let key = Sq::new(ns, db, &self.name);
		txn.set(&key, &sq, None).await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}
