use std::borrow::Cow;
use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;
use tracing::instrument;

use crate::catalog::Error;
use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt;
use crate::expr::Base;
use crate::expr::statements::alter::sequence::AlterSequenceStatement;
use crate::iam::{Action, ResourceKind};
use crate::key::database::all::DatabaseRoot;
use crate::key::database::sq::Sq;
use crate::legacy::expr_to_ident;
use crate::val::{Duration, Value};

#[instrument(level = "trace", name = "AlterSequenceStatement::compute", skip_all)]
pub(crate) async fn alter_sequence_statement_compute(
	this: &AlterSequenceStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Sequence, Base::Db)?;
	let name = expr_to_ident(stk, ctx, opt, doc, &this.name, "sequence name").await?;
	// Get the NS and DB
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	// Fetch the transaction
	let txn = ctx.tx();
	// Get the sequence definition
	let mut sq = match txn.get_db_sequence(ns, db, &name, None).await {
		Ok(tb) => tb.deref().clone(),
		Err(e) => {
			if this.if_exists && matches!(e.downcast_ref(), Some(Error::SeqNotFound { .. })) {
				return Ok(Value::None);
			} else {
				return Err(e);
			}
		}
	};

	if let Some(timeout) = &this.timeout {
		// Process the statement
		if let Some(timeout) = stk
			.run(|stk| crate::legacy::expr_compute(timeout, stk, ctx, opt, doc))
			.await
			.catch_return()?
			.cast_to::<Option<Duration>>()?
		{
			sq.timeout = Some(timeout.0);
		} else {
			sq.timeout = None;
		}
	}
	// Set the sequence definition
	let key = Sq {
		prefix: DatabaseRoot {
			ns,
			db,
		},
		sq: Cow::Borrowed(&name),
	};
	txn.set_key(&key, &sq).await?;
	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
