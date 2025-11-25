use std::sync::Arc;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;
use uuid::Uuid;

use super::DefineKind;
use crate::catalog::providers::{CatalogProvider, TableProvider};
use crate::catalog::{Index, IndexDefinition, TableDefinition, TableId};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::{expr_to_ident, exprs_to_fields};
use crate::expr::{Base, Expr, Literal, Part};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefineIndexStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub what: Expr,
	pub cols: Vec<Expr>,
	pub index: Index,
	pub comment: Option<Expr>,
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
			comment: None,
			concurrently: false,
		}
	}
}

impl DefineIndexStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Index, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();

		// Compute name and what
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "index name").await?;
		let what = expr_to_ident(stk, ctx, opt, doc, &self.what, "index table").await?;

		// Ensure the table exists
		let (ns, db) = opt.ns_db()?;
		let tb = txn.ensure_ns_db_tb(Some(ctx), ns, db, &what).await?;

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

		// Process the statement
		let index_def = IndexDefinition {
			index_id,
			name,
			table_name: what,
			cols: cols.clone(),
			index: self.index.clone(),
			comment: map_opt!(x as &self.comment => compute_to!(stk, ctx, opt, doc, x => String)),
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
		// Process the index
		run_indexing(ctx, opt, tb.table_id, index_def.into(), !self.concurrently).await?;

		// Ok all good
		Ok(Value::None)
	}
}
pub(in crate::expr::statements) async fn run_indexing(
	ctx: &Context,
	opt: &Options,
	tb: TableId,
	ix: Arc<IndexDefinition>,
	blocking: bool,
) -> Result<()> {
	let rcv = ctx
		.get_index_builder()
		.ok_or_else(|| Error::unreachable("No Index Builder"))?
		.build(ctx, opt.clone(), tb, ix, blocking)
		.await?;
	if let Some(rcv) = rcv {
		rcv.await.map_err(|_| Error::IndexingBuildingCancelled)?
	} else {
		Ok(())
	}
}
