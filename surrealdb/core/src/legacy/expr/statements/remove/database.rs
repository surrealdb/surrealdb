use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::Error;
use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::expr::statements::remove::database::RemoveDatabaseStatement;
use crate::iam::{Action, ResourceKind};
use crate::legacy::expr::statements::remove::retire_database_indexes;
use crate::legacy::{expr_to_ident, kill_database_subscriptions};
use crate::val::Value;

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "RemoveDatabaseStatement::compute", skip_all)]
pub(crate) async fn remove_database_statement_compute(
	this: &RemoveDatabaseStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Database, Base::Ns)?;
	// Get the transaction
	let txn = ctx.tx();

	// Compute the name
	let name = expr_to_ident(stk, ctx, opt, doc, &this.name, "database name").await?;
	let ns = opt.ns()?;
	let db = match txn.get_db_by_name(ns, &name, None).await? {
		Some(x) => x,
		None => {
			if this.if_exists {
				return Ok(Value::None);
			} else {
				return Err(Error::DbNotFound {
					name,
				}
				.into());
			}
		}
	};

	// Retire index state before deleting the database definition. Durable
	// cleanup is transactional; local builder aborts are deferred until commit.
	retire_database_indexes(ctx, &txn, db.namespace_id, db.database_id).await?;
	// Tell every subscriber in the database that it is going away. The
	// deferred delete below takes the whole `/*{ns}*{db}` prefix, `lq` rows
	// included, so nothing else would ever wake these clients.
	kill_database_subscriptions(ctx, &txn, db.namespace_id, db.database_id).await?;
	// Remove the sequences
	if let Some(seq) = ctx.get_sequences() {
		seq.database_removed(&txn, db.namespace_id, db.database_id).await?;
	}

	// Delete the catalog definition and enqueue the data for background
	// reclaim. Only the small catalog entry is removed in this transaction
	// (so the database is immediately unreachable); the potentially huge
	// `/*{ns}*{db}` data prefix is destroyed asynchronously by
	// `Datastore::reclaim_tombstones`. This keeps `REMOVE DATABASE` fast
	// and bounded regardless of how much data the database holds, and a
	// rollback undoes the removal without having destroyed any data.
	txn.del_db_deferred(ns, &db.name, this.expunge).await?;

	// Clear the cache
	if let Some(cache) = ctx.get_cache() {
		cache.clear();
	}
	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
