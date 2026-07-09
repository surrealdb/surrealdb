use std::sync::Arc;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;
use uuid::Uuid;

use super::DefineKind;
use crate::catalog::providers::TableProvider;
use crate::catalog::{INDEX_FORMAT_VERSION, Index, IndexDefinition, TableDefinition, TableId};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::{expr_to_ident, exprs_to_fields};
use crate::expr::{Base, Expr, FlowResultExt, Idiom, Literal, Part};
use crate::iam::{Action, ResourceKind};
use crate::idx::docids::TableDocIds;
use crate::kvs::Transaction;
use crate::kvs::index::{IndexBuilder, retire_durable_index};
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
		ctx.is_allowed(opt, Action::Edit, ResourceKind::Index, Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();

		// Compute name and what
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "index name").await?;
		let table_name =
			TableName::new(expr_to_ident(stk, ctx, opt, doc, &self.what, "index table").await?);

		// Ensure the table exists
		let (ns, db) = opt.ns_db()?;
		let tb = txn.get_or_add_tb(Some(ctx), ns, db, &table_name, None).await?;

		// Check if the definition exists
		let existing =
			txn.get_tb_index(tb.namespace_id, tb.database_id, &tb.name, &name, None).await?;
		if existing.is_some() {
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
		}

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
				txn.get_tb_field(tb.namespace_id, tb.database_id, &tb.name, &fd, None).await?
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
							txn.get_tb_field(tb.namespace_id, tb.database_id, &tb.name, first, None).await?
						&& f.field_kind.as_ref().is_none_or(|k| k.allows_sub_fields())
			{
				// Sub-fields of computed fields cannot be indexed
				if f.computed.is_some() {
					bail!(Error::ComputedFieldCannotBeIndexed {
						field: first.as_str().to_owned(),
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

		if let Some(ix) = existing.as_ref()
			&& self.kind == DefineKind::Default
			&& opt.import
			&& import_replay_can_reuse_index(ix, &table_name, &cols, &self.index)
		{
			// Import replays are idempotent when the physical index definition
			// already matches. Preserve data and durable build state while still
			// allowing metadata such as comments to be refreshed. Preserve the
			// existing on-disk `format_version` too: this path reuses the existing
			// index data without rebuilding, so a stale doc-ID-backed index must
			// stay stale (and keep failing `ensure_current_format`) rather than be
			// falsely stamped current and read through the new table-level resolver.
			let index_def = IndexDefinition {
				index_id: ix.index_id,
				name: name.into(),
				table_name,
				cols,
				index: self.index.clone(),
				comment,
				prepare_remove: false,
				format_version: ix.format_version,
			};
			txn.put_tb_index(tb.namespace_id, tb.database_id, &tb.name, &index_def).await?;
			refresh_table_index_cache(ctx, &txn, ns, db, &tb).await?;
			return Ok(Value::None);
		}

		// If an index with this name already exists, this is a destructive
		// replacement: either `Overwrite`, or an import replay whose physical
		// definition changed (a plain redefine over an existing index has already
		// bailed at the guard above, and an idempotent import replay returned at
		// the fast path). Retire the old index's durable state and delete its data
		// here, synchronously, before the new index is rebuilt — delete-before-
		// rebuild keeps peak disk roughly flat for an inline rebuild instead of
		// holding the old and new index data simultaneously.
		if let Some(ix) = existing.as_ref() {
			// Decide up front — before the catalog is mutated — whether this
			// destructive replacement drops the table's LAST doc-ID-consuming
			// index (full-text / HNSW / DiskAnn) with no replacement consumer
			// taking its place. If so, reclaim the shared table-level doc-ID
			// space here, exactly as RemoveIndexStatement does; otherwise the
			// `!di`/`!dd` mappings leak (record deletes stop calling
			// `remove_doc_id` once no consumer remains, and a doc-ID index defined
			// later could reuse a deleted record's id). A replacement that is
			// itself a doc-ID index keeps consuming the space, so it is preserved.
			let purge_table_doc_ids =
				matches!(ix.index, Index::FullText(_) | Index::Hnsw(_) | Index::DiskAnn(_))
					&& !matches!(
						self.index,
						Index::FullText(_) | Index::Hnsw(_) | Index::DiskAnn(_)
					) && !txn
					.all_tb_indexes(tb.namespace_id, tb.database_id, &tb.name, None)
					.await?
					.iter()
					.any(|other| {
						other.index_id != ix.index_id
							&& matches!(
								other.index,
								Index::FullText(_) | Index::Hnsw(_) | Index::DiskAnn(_)
							)
					});
			// Clear process-local index wrappers without aborting the current
			// durable builder here. Durable state and catalog entries are
			// retired atomically in this schema transaction below, and the
			// process-local builder abort is deferred until commit.
			ctx.get_index_stores().index_removed(tb.namespace_id, tb.database_id, &tb, ix).await?;
			if let Some(index_builder) = ctx.get_index_builder() {
				txn.register_index_builder_abort_after_commit(
					index_builder.clone(),
					tb.namespace_id,
					tb.database_id,
					tb.name.clone(),
					ix.index_id,
				)
				.await;
			}
			retire_durable_index(&txn, tb.namespace_id, tb.database_id, &tb.name, ix.index_id)
				.await?;
			txn.del_tb_index(tb.namespace_id, tb.database_id, &tb.name, &name).await?;
			if purge_table_doc_ids {
				TableDocIds::new(tb.namespace_id, tb.database_id, tb.name.clone())
					.remove_all(&txn)
					.await?;
			}
			// Serialize concurrent last-consumer replacements on last-writer-wins
			// backends (TiKV). The `purge_table_doc_ids` decision above is read
			// from a range scan of the index list, which those backends do not
			// validate for write-conflicts; two `DEFINE INDEX OVERWRITE`s
			// replacing the final two doc-ID consumers with non-doc-ID indexes
			// could each see the other still present and both skip the purge,
			// leaking the shared `!di`/`!dd` mappings. Reading the
			// table-definition key here arms the write-conflict check against the
			// `put_tb` in `refresh_table_index_cache` below (which targets the
			// same key): the second committer is rejected, its whole transaction
			// rolls back, and it re-evaluates the decision on retry as the sole
			// remaining replacer. On conflict-serializing backends the `put_tb`
			// write already serializes them.
			let tb_key = crate::key::database::tb::TableKey {
				prefix: crate::key::database::all::DatabaseRoot {
					ns: tb.namespace_id,
					db: tb.database_id,
				},
				tb: std::borrow::Cow::Borrowed(&tb.name),
			};
			let _ = txn.get_key(&tb_key, None).await?;
		}
		// A (re)defined index always gets a fresh internal id, so the retired
		// index's durable state and generation-scoped queues can never be mistaken
		// for the new definition.
		let index_id = ctx
			.try_get_sequences()?
			.next_index_id(Some(ctx), tb.namespace_id, tb.database_id, tb.name.clone())
			.await?;

		// Process the statement
		let index_def = IndexDefinition {
			index_id,
			name: name.clone().into(),
			table_name,
			cols: cols.clone(),
			index: self.index.clone(),
			comment,
			prepare_remove: false,
			format_version: INDEX_FORMAT_VERSION,
		};
		txn.put_tb_index(tb.namespace_id, tb.database_id, &tb.name, &index_def).await?;

		refresh_table_index_cache(ctx, &txn, ns, db, &tb).await?;
		let index_builder =
			ctx.get_index_builder().ok_or_else(|| Error::unreachable("No Index Builder"))?;
		txn.register_uncommitted_index_build_cleanup(
			index_builder.clone(),
			index_builder.transaction_factory(),
			tb.namespace_id,
			tb.database_id,
			tb.name.clone(),
			index_id,
		)
		.await;
		// Process the index
		run_indexing_with_builder(
			index_builder,
			ctx,
			opt,
			tb.table_id,
			index_def.into(),
			!self.concurrently,
		)
		.await?;

		// Ok all good
		Ok(Value::None)
	}
}

fn import_replay_can_reuse_index(
	ix: &IndexDefinition,
	table_name: &TableName,
	cols: &[Idiom],
	index: &Index,
) -> bool {
	!ix.prepare_remove
		&& &ix.table_name == table_name
		&& ix.cols.as_slice() == cols
		&& &ix.index == index
}

pub(in crate::expr::statements) async fn refresh_table_index_cache(
	_ctx: &FrozenContext,
	txn: &Transaction,
	ns: &str,
	db: &str,
	tb: &TableDefinition,
) -> Result<()> {
	txn.put_tb(
		ns,
		db,
		&TableDefinition {
			cache_indexes_ts: Uuid::now_v7(),
			..tb.clone()
		},
	)
	.await?;

	txn.clear_cache();
	Ok(())
}

pub(in crate::expr::statements) async fn run_indexing(
	ctx: &FrozenContext,
	opt: &Options,
	tb: TableId,
	ix: Arc<IndexDefinition>,
	blocking: bool,
) -> Result<()> {
	let index_builder =
		ctx.get_index_builder().ok_or_else(|| Error::unreachable("No Index Builder"))?;
	run_indexing_with_builder(index_builder, ctx, opt, tb, ix, blocking).await
}

async fn run_indexing_with_builder(
	index_builder: &IndexBuilder,
	ctx: &FrozenContext,
	opt: &Options,
	tb: TableId,
	ix: Arc<IndexDefinition>,
	blocking: bool,
) -> Result<()> {
	let rcv = index_builder.build(ctx, opt.clone(), tb, ix, blocking).await?;
	if let Some(rcv) = rcv {
		rcv.await.map_err(|_| Error::IndexingBuildingCancelled {
			reason: "Channel shutdown".to_string(),
		})?
	} else {
		Ok(())
	}
}
