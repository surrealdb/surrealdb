use std::borrow::Cow;

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::catalog::providers::{CatalogProvider, DatabaseProvider};
use crate::catalog::{Error as CatalogError, SequenceDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt;
use crate::exec::Error as ExecError;
use crate::expr::Base;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::define::sequence::DefineSequenceStatement;
use crate::iam::{Action, ResourceKind};
use crate::key::schema::{SeqBatchPrefix, SeqStatePrefix, SequenceKey};
use crate::legacy::expr_to_ident;
use crate::val::{Duration, Value};

#[instrument(level = "trace", name = "DefineSequenceStatement::compute", skip_all)]
pub(crate) async fn define_sequence_statement_compute(
	this: &DefineSequenceStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Sequence, Base::Db)?;
	// Compute name
	let name = expr_to_ident(stk, ctx, opt, doc, &this.name, "sequence name").await?;
	// Compute timeout
	let timeout = stk
		.run(|stk| crate::legacy::expr_compute(&this.timeout, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to::<Option<Duration>>()?
		.map(|x| x.0);
	// Fetch the transaction
	let txn = ctx.tx();
	let (ns, db) = ctx.get_ns_db_ids(opt).await?;
	// Check if the definition exists
	if txn.get_db_sequence(ns, db, &name, None).await.is_ok() {
		match this.kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(CatalogError::SeqAlreadyExists {
						name: name.clone(),
					});
				}
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => {
				return Ok(Value::None);
			}
		}
	}

	let db = {
		let (ns, db) = opt.ns_db()?;
		txn.get_or_add_db(Some(ctx), ns, db).await?
	};

	// Process the statement
	let key = SequenceKey {
		ns: db.namespace_id,
		db: db.database_id,
		sq: Cow::Borrowed(&name),
	};

	let batch = stk
		.run(|stk| crate::legacy::expr_compute(&this.batch, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to::<i64>()?;

	let Ok(batch) = u32::try_from(batch) else {
		bail!(ExecError::Query {
			message: format!(
				"`{batch}` is not valid batch size for a sequence definition. A batch size must be within 0..={}",
				u32::MAX
			),
		})
	};

	let sq = SequenceDefinition {
		name: name.clone().into(),
		batch,
		start: stk
			.run(|stk| crate::legacy::expr_compute(&this.start, stk, ctx, opt, doc))
			.await
			.catch_return()?
			.cast_to()?,
		timeout,
	};
	// Set the definition
	txn.set_key(&key, &sq).await?;

	// Clear any pre-existing sequence records
	let ba_range = SeqBatchPrefix {
		ns: db.namespace_id,
		db: db.database_id,
		sq: Cow::Borrowed(&sq.name),
	}
	.range()?;
	txn.delr(ba_range).await?;
	let st_range = SeqStatePrefix {
		ns: db.namespace_id,
		db: db.database_id,
		sq: Cow::Borrowed(&sq.name),
	}
	.range()?;
	txn.delr(st_range).await?;

	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
