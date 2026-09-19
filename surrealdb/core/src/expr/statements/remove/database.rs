use anyhow::Result;
use reblessive::tree::Stk;

use super::{kill_database_lives, retire_database_indexes};
use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, Literal, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct RemoveDatabaseStatement {
	pub name: Expr,
	pub if_exists: bool,
	pub expunge: bool,
}

impl Default for RemoveDatabaseStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
			expunge: false,
		}
	}
}

impl RemoveDatabaseStatement {
	/// Process this type returning a computed simple Value
	#[instrument(level = "trace", name = "RemoveDatabaseStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
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
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "database name").await?;
		let ns = opt.ns()?;
		let db = match txn.get_db_by_name(ns, &name, None).await? {
			Some(x) => x,
			None => {
				if self.if_exists {
					return Ok(Value::None);
				} else {
					return Err(Error::DbNotFound {
						name,
					}
					.into());
				}
			}
		};

		// Announce the subscriptions this removal destroys, while the catalog
		// entries naming them still exist.
		kill_database_lives(ctx, &txn, db.namespace_id, db.database_id).await?;
		// Retire index state before deleting the database definition. Durable
		// cleanup is transactional; local builder aborts are deferred until commit.
		retire_database_indexes(ctx, &txn, db.namespace_id, db.database_id).await?;
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
		txn.del_db_deferred(ns, &db.name, self.expunge).await?;

		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear();
		}
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}
