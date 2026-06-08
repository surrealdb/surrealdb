use anyhow::Result;
use reblessive::tree::Stk;

use super::retire_database_indexes;
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

		// Retire index state before deleting the database definition. Durable
		// cleanup is transactional; local builder aborts are deferred until commit.
		retire_database_indexes(ctx, &txn, db.namespace_id, db.database_id).await?;
		// Remove the sequences
		if let Some(seq) = ctx.get_sequences() {
			seq.database_removed(&txn, db.namespace_id, db.database_id).await?;
		}

		// Delete the definition.
		//
		// The transactional `del_db` path (which eventually calls
		// `delp` / `clrp`) is bounded by `SURREAL_TIKV_DELR_MAX_KEYS`
		// (default 1M) on TiKV. If the database is larger than that
		// the call below returns `TransactionRangeTooLarge` and the
		// outer transaction is rolled back — both the metadata clear
		// and the partial prefix-delete are undone together, so the
		// database is left intact (and reachable via the catalog)
		// afterwards.
		//
		// Operators hitting that cap have two escape hatches:
		//   1. Raise `SURREAL_TIKV_DELR_MAX_KEYS` for this datastore instance and re-issue the
		//      `REMOVE DATABASE` statement.
		//   2. Run [`crate::kvs::Datastore::unsafe_destroy_range`] against the database key prefix
		//      *first*, shrinking the data side to something the bounded delete can swallow, then
		//      re-issue `REMOVE DATABASE`. The catalog metadata still points at the (now-empty)
		//      prefix during this window, so the unsafe destroy is consistent with what the
		//      statement is about to do anyway.
		txn.del_db(ns, &db.name, self.expunge).await?;

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
