use std::sync::Arc;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;
use uuid::Uuid;

use super::DefineKind;
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, Index, IndexDefinition, NamespaceId, TableDefinition, TableId};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::{expr_to_ident, exprs_to_fields};
use crate::expr::{Base, Expr, FlowResultExt, Literal, Part};
use crate::iam::{Action, ResourceKind};
use crate::val::{TableName, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefineIndexStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub what: Expr,
	pub cols: Vec<Expr>,
	pub index: Index,
	pub comment: Expr,
	pub concurrently: bool,
}

impl Default for DefineIndexStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			what: Expr::Literal(Literal::None),
			cols: Vec::new(),
			index: Index::Idx,
			comment: Expr::Literal(Literal::None),
			concurrently: false,
		}
	}
}

impl DefineIndexStatement {
	/// Process this type returning a computed simple Value
	#[instrument(level = "trace", name = "DefineIndexStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Index, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();

		// Compute name and what
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "index name").await?;
		let table_name =
			TableName::new(expr_to_ident(stk, ctx, opt, doc, &self.what, "index table").await?);

		// Ensure the table exists
		let (ns, db) = opt.ns_db()?;
		let tb = txn.get_or_add_tb(Some(ctx), ns, db, &table_name).await?;

		// Check if the definition exists
		let index_id = if let Some(ix) =
			txn.get_tb_index(tb.namespace_id, tb.database_id, &tb.name, &name).await?
		{
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::IxAlreadyExists {
							name: self.name.to_sql(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => return Ok(Value::None),
			}
			// Clear the index store cache
			ctx.get_index_stores()
				.index_removed(ctx.get_index_builder(), tb.namespace_id, tb.database_id, &tb, &ix)
				.await?;
			ix.index_id
		} else {
			ctx.try_get_sequences()?
				.next_index_id(Some(ctx), tb.namespace_id, tb.database_id, tb.name.clone())
				.await?
		};

		// Compute columns
		let cols = exprs_to_fields(stk, ctx, opt, doc, self.cols.as_slice()).await?;

		// If the table is schemafull, ensure that the fields exist.
		if tb.schemafull {
			// Check that the fields exist
			for idiom in cols.iter() {
				// TODO: Was this correct? Can users not index data on sub-fields?
				let Some(Part::Field(first)) = idiom.0.first() else {
					continue;
				};

				//
				if txn
					.get_tb_field(tb.namespace_id, tb.database_id, &tb.name, first)
					.await?
					.is_none()
				{
					bail!(Error::FdNotFound {
						name: idiom.to_raw_string(),
					});
				}
			}
		}

		let comment = stk
			.run(|stk| self.comment.compute(stk, ctx, opt, doc))
			.await
			.catch_return()?
			.cast_to()?;

		// Process the statement
		let index_def = IndexDefinition {
			index_id,
			name,
			table_name,
			cols: cols.clone(),
			index: self.index.clone(),
			comment,
			prepare_remove: false,
		};
		txn.put_tb_index(tb.namespace_id, tb.database_id, &tb.name, &index_def).await?;

		// Refresh the table cache
		txn.put_tb(
			ns,
			db,
			&TableDefinition {
				cache_indexes_ts: Uuid::now_v7(),
				..tb.as_ref().clone()
			},
		)
		.await?;

		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(tb.namespace_id, tb.database_id, &tb.name);
		}
		// Clear the cache
		txn.clear_cache();

		// Commit the index definition BEFORE running index building.
		// This prevents transaction conflicts when bulk indexing creates millions
		// of sequence numbers - the original transaction's snapshot would be stale
		// by the time it tries to commit after index building completes.
		txn.commit().await?;

		// Process the index (uses its own transactions internally)
		run_indexing(
			ctx,
			opt,
			tb.namespace_id,
			tb.database_id,
			tb.table_id,
			index_def.into(),
			!self.concurrently,
		)
		.await?;

		// Ok all good
		Ok(Value::None)
	}
}
pub(in crate::expr::statements) async fn run_indexing(
	ctx: &FrozenContext,
	opt: &Options,
	ns: NamespaceId,
	db: DatabaseId,
	tb: TableId,
	ix: Arc<IndexDefinition>,
	blocking: bool,
) -> Result<()> {
	let rcv = ctx
		.get_index_builder()
		.ok_or_else(|| Error::unreachable("No Index Builder"))?
		.build(ctx, opt.clone(), ns, db, tb, ix, blocking)
		.await?;
	if let Some(rcv) = rcv {
		rcv.await.map_err(|_| Error::IndexingBuildingCancelled {
			reason: "Channel shutdown".to_string(),
		})?
	} else {
		Ok(())
	}
}
