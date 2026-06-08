use anyhow::Result;
use reblessive::tree::Stk;
use uuid::Uuid;

use crate::catalog::TableDefinition;
use crate::catalog::providers::TableProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, Literal, Value};
use crate::iam::{Action, ResourceKind};
use crate::kvs::index::retire_durable_index;
use crate::val::TableName;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct RemoveIndexStatement {
	pub name: Expr,
	pub what: Expr,
	pub if_exists: bool,
}

impl Default for RemoveIndexStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			what: Expr::Literal(Literal::None),
			if_exists: false,
		}
	}
}

impl RemoveIndexStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		ctx.is_allowed(opt, Action::Edit, ResourceKind::Index, Base::Db)?;
		// Compute the name
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "index name").await?;
		// Compute the what
		let table_name =
			TableName::new(expr_to_ident(stk, ctx, opt, doc, &self.what, "what").await?);
		// Get the NS and DB
		let (ns_name, db_name) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		// Get the transaction
		let txn = ctx.tx();
		// Get the index definition
		let res = txn.expect_tb_index(ns, db, &table_name, &name).await;
		let ix = match res {
			Err(e) => {
				if self.if_exists && matches!(e.downcast_ref(), Some(Error::IxNotFound { .. })) {
					return Ok(Value::None);
				}
				return Err(e);
			}
			Ok(ix) => ix,
		};
		// Get the table definition
		let tb = txn.expect_tb(ns, db, &table_name).await?;
		// Clear process-local index wrappers immediately, then retire durable
		// build state in the same transaction that removes the catalog
		// definition. The builder abort is deferred until this transaction
		// commits so rollback/cancel keeps an in-flight build alive.
		ctx.get_index_stores().index_removed(ns, db, &tb, &ix).await?;
		if let Some(index_builder) = ctx.get_index_builder() {
			txn.register_index_builder_abort_after_commit(
				index_builder.clone(),
				ns,
				db,
				table_name.clone(),
				ix.index_id,
			)
			.await;
		}
		retire_durable_index(&txn, ns, db, &table_name, ix.index_id).await?;
		// Delete the index data.
		txn.del_tb_index(ns, db, &table_name, &name).await?;
		// Refresh the table cache for indexes
		txn.put_tb(
			ns_name,
			db_name,
			&TableDefinition {
				cache_indexes_ts: Uuid::now_v7(),
				..tb.as_ref().clone()
			},
		)
		.await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}
