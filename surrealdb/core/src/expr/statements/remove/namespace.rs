use anyhow::Result;
use reblessive::tree::Stk;

use super::retire_namespace_indexes;
use crate::catalog::Error;
use crate::catalog::providers::NamespaceProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::statements::subscriptions::kill_namespace_subscriptions;
use crate::expr::{Base, Expr, Literal, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct RemoveNamespaceStatement {
	pub name: Expr,
	pub if_exists: bool,
	pub expunge: bool,
}

impl Default for RemoveNamespaceStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
			expunge: false,
		}
	}
}

impl RemoveNamespaceStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		ctx.is_allowed(opt, Action::Edit, ResourceKind::Namespace, Base::Root)?;
		// Get the transaction
		let txn = ctx.tx();
		// Compute the name
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "namespace name").await?;
		let ns = match txn.get_ns_by_name(&name, None).await? {
			Some(x) => x,
			None => {
				if self.if_exists {
					return Ok(Value::None);
				}

				return Err(Error::NsNotFound {
					name,
				}
				.into());
			}
		};

		// Retire index state before deleting the namespace definition. Durable
		// cleanup is transactional; local builder aborts are deferred until commit.
		retire_namespace_indexes(ctx, &txn, ns.namespace_id).await?;
		// Tell every subscriber in the namespace that it is going away. The
		// deferred delete below takes the whole `/*{ns}` prefix, `lq` rows
		// included, so nothing else would ever wake these clients.
		kill_namespace_subscriptions(ctx, &txn, ns.namespace_id).await?;
		// Remove the sequences
		if let Some(seq) = ctx.get_sequences() {
			seq.namespace_removed(&txn, ns.namespace_id).await?;
		}

		// Delete the catalog definition and enqueue the data for background
		// reclaim. Only the small catalog entry is removed in this transaction
		// (so the namespace is immediately unreachable); the potentially huge
		// `/*{ns}` data prefix is destroyed asynchronously by
		// `Datastore::reclaim_tombstones`. This keeps `REMOVE NAMESPACE` fast
		// and bounded regardless of how much data the namespace holds, and a
		// rollback undoes the removal without having destroyed any data.
		txn.del_ns_deferred(&ns.name, self.expunge).await?;

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
