use std::sync::Arc;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;
use uuid::Uuid;

use super::DefineKind;
use crate::catalog::providers::TableProvider;
use crate::catalog::{Index, IndexDefinition, TableDefinition, TableId};
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

		// Validate each indexed field:
		// 1. Computed fields cannot be indexed (regardless of schemafull/schemaless). This applies
		//    to both exact field matches and sub-field paths whose parent is a computed field.
		// 2. If the table is schemafull, ensure that every indexed field is defined. For sub-field
		//    paths (e.g. `document.visible`), we allow the index if either the full path is
		//    explicitly defined, or the top-level parent field has a type that permits sub-field
		//    access — this includes `object`, `any`, literal object types (e.g. `{ key: string }`),
		//    and union types where every non-none variant is object-like. A parent field with no
		//    explicit type is also accepted, since it is unconstrained.
		for idiom in cols.iter() {
			let fd = idiom.to_raw_string();
			// Check if the exact field path (e.g. `document.visible`) is defined
			if let Some(f) =
				txn.get_tb_field(tb.namespace_id, tb.database_id, &tb.name, &fd).await?
			{
				// Computed fields cannot be indexed
				if f.computed.is_some() {
					bail!(Error::ComputedFieldCannotBeIndexed {
						field: fd,
						index: name
					});
				}
				continue;
			}
			// For sub-field paths, extract the top-level parent field name
			if let Some(Part::Field(first)) = idiom.0.first() &&
						// Allow the index when the parent field exists and its type
						// permits sub-field access. If no type is set (field_kind is
						// None), the field is unconstrained and sub-fields are allowed.
						let Some(f) =
							txn.get_tb_field(tb.namespace_id, tb.database_id, &tb.name, first).await?
						&& f.field_kind.as_ref().is_none_or(|k| k.allows_sub_fields())
			{
				// Sub-fields of computed fields cannot be indexed
				if f.computed.is_some() {
					bail!(Error::ComputedFieldCannotBeIndexed {
						field: first.to_string(),
						index: name
					});
				}
				continue;
			}
			if tb.schemafull {
				bail!(Error::FdNotFound {
					name: idiom.to_raw_string(),
				});
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
		// Process the index
		run_indexing(ctx, opt, tb.table_id, index_def.into(), !self.concurrently).await?;

		// Ok all good
		Ok(Value::None)
	}
}
pub(in crate::expr::statements) async fn run_indexing(
	ctx: &FrozenContext,
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
		rcv.await.map_err(|_| Error::IndexingBuildingCancelled {
			reason: "Channel shutdown".to_string(),
		})?
	} else {
		Ok(())
	}
}
