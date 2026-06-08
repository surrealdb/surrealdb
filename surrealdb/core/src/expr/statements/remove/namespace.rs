use anyhow::Result;
use reblessive::tree::Stk;

use super::retire_namespace_indexes;
use crate::catalog::providers::NamespaceProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
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
		// Remove the sequences
		if let Some(seq) = ctx.get_sequences() {
			seq.namespace_removed(&txn, ns.namespace_id).await?;
		}

		// Delete the definition.
		//
		// The transactional `delp` / `clrp` path inside `del_ns` is
		// bounded by `SURREAL_TIKV_DELR_MAX_KEYS` (default 1M) on TiKV.
		// If the namespace is larger than that the call below returns
		// `TransactionRangeTooLarge` and the outer transaction is
		// rolled back — both the metadata clear and the partial
		// prefix-delete are undone together, so the namespace is left
		// intact (and reachable via the catalog) afterwards.
		//
		// Operators hitting that cap have two escape hatches:
		//   1. Raise `SURREAL_TIKV_DELR_MAX_KEYS` for this datastore instance and re-issue the
		//      `REMOVE NAMESPACE` statement.
		//   2. Run [`crate::kvs::Datastore::unsafe_destroy_range`] against the namespace key prefix
		//      *first*, shrinking the data side to something the bounded delete can swallow, then
		//      re-issue `REMOVE NAMESPACE`. The catalog metadata still points at the (now-empty)
		//      prefix during this window, so the unsafe destroy is consistent with what the
		//      statement is about to do anyway.
		txn.del_ns(&ns.name, self.expunge).await?;

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
