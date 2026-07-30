use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::catalog::{DatabaseDefinition, Error};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt;
use crate::expr::Base;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::define::database::DefineDatabaseStatement;
use crate::iam::{Action, ResourceKind};
use crate::legacy::expr_to_ident;
use crate::val::Value;

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "DefineDatabaseStatement::compute", skip_all)]
pub(crate) async fn define_database_statement_compute(
	this: &DefineDatabaseStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Database, Base::Ns)?;

	// Get the NS
	let ns = opt.ns()?;

	// Fetch the transaction
	let txn = ctx.tx();
	let nsv = txn.get_or_add_ns(Some(ctx), ns).await?;

	// Process the name
	let name = expr_to_ident(stk, ctx, opt, doc, &this.name, "database name").await?;

	// Check if the definition exists
	let database_id = if let Some(db) = txn.get_db_by_name(ns, &name, None).await? {
		match this.kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(Error::DbAlreadyExists {
						name: name.clone(),
					});
				}
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => {
				return Ok(Value::None);
			}
		}

		db.database_id
	} else {
		ctx.try_get_sequences()?.next_database_id(Some(ctx), nsv.namespace_id).await?
	};

	let comment = stk
		.run(|stk| crate::legacy::expr_compute(&this.comment, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to()?;

	// Set the database definition, keyed by namespace name and database name.
	let db_def = DatabaseDefinition {
		namespace_id: nsv.namespace_id,
		database_id,
		name: name.clone().into(),
		comment,
		changefeed: this.changefeed,
		strict: this.strict,
	};
	txn.put_db(nsv.name.as_str(), db_def).await?;

	// Clear the cache
	if let Some(cache) = ctx.get_cache() {
		cache.clear();
	}

	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
