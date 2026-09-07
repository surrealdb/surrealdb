use std::sync::Arc;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;
use uuid::Uuid;

use crate::catalog::providers::TableProvider;
use crate::catalog::{
	Error as CatalogError, INDEX_FORMAT_VERSION, IndexDefinition, TableDefinition, TableId,
};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::EngineError;
use crate::exe::FlowResultExt;
use crate::exec::Error as ExecError;
use crate::expr::index_kind::Index;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::define::index::DefineIndexStatement;
use crate::expr::{Base, Idiom, Part};
use crate::iam::{Action, ResourceKind};
use crate::kvs::index::{
	AbortLocalBuild, CleanUncommittedBuild, IndexBuilder, retire_durable_index,
};
use crate::kvs::tx::DocIdsReclaimClaim;
use crate::kvs::{DatastoreError, Transaction};
use crate::legacy::{expr_to_ident, exprs_to_fields};
use crate::val::{TableName, Value};

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "DefineIndexStatement::compute", skip_all)]
pub(crate) async fn define_index_statement_compute(
	this: &DefineIndexStatement,
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
	let name = expr_to_ident(stk, ctx, opt, doc, &this.name, "index name").await?;
	let table_name =
		TableName::new(expr_to_ident(stk, ctx, opt, doc, &this.what, "index table").await?);

	// Ensure the table exists
	let (ns, db) = opt.ns_db()?;
	let tb = txn.get_or_add_tb(Some(ctx), ns, db, &table_name, None).await?;
	let tb_name = tb.name.clone();

	// A lightweight relation's edges store no records for an index to
	// apply to; the auto `in`/`out` fields are written by the table
	// definition directly, never through this statement.
	anyhow::ensure!(
		crate::kvs::lightweight::lightweight_relation(&tb.table_type).is_none(),
		crate::exec::Error::Thrown(
			"a LIGHTWEIGHT relation cannot carry an index: its edges store no records".to_owned(),
		)
	);

	// Check if the definition exists
	let existing = txn.get_tb_index(tb.namespace_id, tb.database_id, &tb_name, &name, None).await?;
	if existing.is_some() {
		match this.kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(CatalogError::IxAlreadyExists {
						name: this.name.to_sql(),
					});
				}
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => return Ok(Value::None),
		}
	}

	// Compute columns
	let cols = exprs_to_fields(stk, ctx, opt, doc, this.cols.as_slice()).await?;

	// Validate each indexed field:
	// 1. Computed fields cannot be indexed (regardless of schemafull/schemaless). This applies to
	//    both exact field matches and sub-field paths whose parent is a computed field.
	// 2. If the table is schemafull, ensure that every indexed field is defined. For sub-field
	//    paths (e.g. `document.visible`), we allow the index if either the full path is explicitly
	//    defined, or the top-level parent field has a type that permits sub-field access — this
	//    includes `object`, `any`, literal object types (e.g. `{ key: string }`), and union types
	//    where every non-none variant is object-like. A parent field with no explicit type is also
	//    accepted, since it is unconstrained.
	for idiom in cols.iter() {
		let fd = idiom.to_raw_string();
		// Check if the exact field path (e.g. `document.visible`) is defined
		if let Some(f) =
			txn.get_tb_field(tb.namespace_id, tb.database_id, &tb_name, &fd, None).await?
		{
			// Computed fields cannot be indexed
			if f.computed.is_some() {
				bail!(ExecError::ComputedFieldCannotBeIndexed {
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
						txn.get_tb_field(tb.namespace_id, tb.database_id, &tb_name, first, None).await?
					&& f.field_kind.as_ref().is_none_or(|k| k.allows_sub_fields())
		{
			// Sub-fields of computed fields cannot be indexed
			if f.computed.is_some() {
				bail!(ExecError::ComputedFieldCannotBeIndexed {
					field: first.as_str().to_owned(),
					index: name
				});
			}
			continue;
		}
		if tb.schemafull {
			bail!(CatalogError::FdNotFound {
				name: idiom.to_raw_string(),
			});
		}
	}

	let comment = stk
		.run(|stk| crate::legacy::expr_compute(&this.comment, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to()?;

	if let Some(ix) = existing.as_ref()
		&& this.kind == DefineKind::Default
		&& opt.import
		&& import_replay_can_reuse_index(ix, &table_name, &cols, &this.index)
	{
		// Import replays are idempotent when the physical index definition
		// already matches. Preserve data and durable build state while still
		// allowing metadata such as comments to be refreshed. Preserve the
		// existing on-disk `format_version` too: this path reuses the existing
		// index data without rebuilding, so it remains on its legacy layout.
		let index_def = IndexDefinition {
			index_id: ix.index_id,
			name: name.into(),
			table_name,
			cols: cols.clone(),
			index: this.index.clone().into(),
			count_cond: match &this.index {
				Index::Count(Some(cond)) => Some(cond.clone()),
				_ => None,
			},
			comment,
			prepare_remove: false,
			format_version: ix.format_version,
		};
		txn.put_tb_index(tb.namespace_id, tb.database_id, &tb_name, &index_def).await?;
		refresh_table_index_cache(ctx, &txn, ns, db, &tb).await?;
		return Ok(Value::None);
	}

	// A doc-ID-consuming index adopts existing table mappings through
	// `TableDocIds::resolve_or_assign`'s fast path. An untouched queued reclaim
	// can be cancelled atomically here and those mappings remain valid. Once any
	// page has committed, however, the space may be torn; leave the queue intact
	// so the bounded reaper can finish, and refuse to publish a consumer until it
	// has done so.
	if !matches!(this.index, Index::Count(_))
		&& txn.claim_tb_doc_ids_reclaim(tb.namespace_id, tb.database_id, &tb_name).await?
			== DocIdsReclaimClaim::InProgress
	{
		bail!(DatastoreError::QueryNotExecuted {
			message: format!(
				"The shared document-ID space for table `{tb_name}` is still being reclaimed; retry DEFINE INDEX after cleanup completes"
			),
		});
	}

	// If an index with this name already exists, this is a destructive
	// replacement: either `Overwrite`, or an import replay whose physical
	// definition changed (a plain redefine over an existing index has already
	// bailed at the guard above, and an idempotent import replay returned at
	// the fast path). The old index's durable state is retired here, and its
	// data is enqueued for background reclaim.
	//
	// The replacement is built while the retired index's data is still on disk,
	// so peak disk holds both. That is the price of a bounded transaction: an
	// inline delete of the old index would put one write per entry into this
	// transaction, which for a large index does not fit in memory.
	if let Some(ix) = existing.as_ref() {
		// Decide up front — before the catalog is mutated — whether this
		// destructive replacement drops the table's LAST doc-ID-consuming
		// index (full-text / HNSW / DiskAnn / doc-ID-format b-tree) with no
		// replacement consumer taking its place. If so, reclaim the shared
		// table-level doc-ID space here, exactly as RemoveIndexStatement
		// does; otherwise the `!di`/`!dd` mappings leak (record deletes stop
		// calling `remove_doc_id` once no consumer remains, and a doc-ID
		// index defined later could reuse a deleted record's id). A
		// replacement that is itself a doc-ID consumer keeps consuming the
		// space, so it is preserved. The replacement is stamped with the
		// current INDEX_FORMAT_VERSION, so every kind except COUNT is a
		// consumer (b-tree entries carry doc-IDs from
		// BTREE_ENTRY_DOC_IDS_FORMAT_VERSION).
		let replacement_uses_doc_ids = !matches!(this.index, Index::Count(_));
		let purge_table_doc_ids = ix.uses_shared_doc_ids()
			&& !replacement_uses_doc_ids
			&& !tb.graph_doc_ids
			&& !txn
				.all_tb_indexes(tb.namespace_id, tb.database_id, &tb_name, None)
				.await?
				.iter()
				.any(|other| other.index_id != ix.index_id && other.uses_shared_doc_ids());
		// Clear process-local index wrappers without aborting the current
		// durable builder here. Durable state and catalog entries are
		// retired atomically in this schema transaction below, and the
		// process-local builder abort is deferred until commit.
		ctx.get_index_stores().index_removed(tb.namespace_id, tb.database_id, &tb, ix).await?;
		if let Some(index_builder) = ctx.get_index_builder() {
			txn.on_commit(AbortLocalBuild::boxed(
				index_builder.clone(),
				tb.namespace_id,
				tb.database_id,
				tb_name.clone(),
				ix.index_id,
			))
			.await;
		}
		retire_durable_index(&txn, tb.namespace_id, tb.database_id, &tb_name, ix.index_id, false)
			.await?;
		// The replaced index's data prefix is enqueued for background reclaim
		// rather than deleted here, so this transaction stays bounded whatever
		// the index holds. The replacement is given a fresh index id below, so
		// its prefix is disjoint from the one queued here.
		txn.del_tb_index_deferred(tb.namespace_id, tb.database_id, &tb_name, &name).await?;
		if purge_table_doc_ids {
			txn.del_tb_doc_ids_deferred(tb.namespace_id, tb.database_id, &tb_name).await?;
		}
		// Concurrent last-consumer replacements are serialized by the locked
		// table-definition read inside `refresh_table_index_cache`'s
		// `replace_tb` below. The `purge_table_doc_ids` decision above is
		// read from a range scan of the index list, which last-writer-wins
		// backends (TiKV) do not validate for write-conflicts; two `DEFINE
		// INDEX OVERWRITE`s replacing the final two doc-ID consumers with
		// non-doc-ID indexes could otherwise each see the other still
		// present and both skip the purge, leaking the shared `!di`/`!dd`
		// mappings. The second committer is rejected, its whole transaction
		// rolls back, and it re-evaluates the decision on retry as the sole
		// remaining replacer.
	}
	// A (re)defined index always gets a fresh internal id, so the retired
	// index's durable state and generation-scoped queues can never be mistaken
	// for the new definition.
	let index_id = ctx
		.try_get_sequences()?
		.next_index_id(Some(ctx), tb.namespace_id, tb.database_id, tb_name.clone())
		.await?;

	// Process the statement
	let index_def = IndexDefinition {
		index_id,
		name: name.clone().into(),
		table_name,
		cols: cols.clone(),
		index: this.index.clone().into(),
		count_cond: match &this.index {
			Index::Count(Some(cond)) => Some(cond.clone()),
			_ => None,
		},
		comment,
		prepare_remove: false,
		format_version: INDEX_FORMAT_VERSION,
	};
	txn.put_tb_index(tb.namespace_id, tb.database_id, &tb_name, &index_def).await?;

	refresh_table_index_cache(ctx, &txn, ns, db, &tb).await?;
	let index_builder =
		ctx.get_index_builder().ok_or_else(|| EngineError::unreachable("No Index Builder"))?;
	txn.on_rollback(CleanUncommittedBuild::boxed(
		index_builder.clone(),
		index_builder.transaction_factory(),
		txn.sequences(),
		tb.namespace_id,
		tb.database_id,
		tb_name.clone(),
		index_id,
	))
	.await;
	// Process the index
	run_indexing_with_builder(
		index_builder,
		ctx,
		opt,
		tb.table_id,
		Arc::new(index_def),
		!this.concurrently,
	)
	.await?;

	// Ok all good
	Ok(Value::None)
}

pub(crate) async fn refresh_table_index_cache(
	_ctx: &FrozenContext,
	txn: &Transaction,
	ns: &str,
	db: &str,
	tb: &TableDefinition,
) -> Result<()> {
	txn.replace_tb(
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

pub(crate) async fn run_indexing(
	ctx: &FrozenContext,
	opt: &Options,
	tb: TableId,
	ix: Arc<IndexDefinition>,
	blocking: bool,
) -> Result<()> {
	let index_builder =
		ctx.get_index_builder().ok_or_else(|| EngineError::unreachable("No Index Builder"))?;
	run_indexing_with_builder(index_builder, ctx, opt, tb, ix, blocking).await
}

pub(crate) async fn run_indexing_with_builder(
	index_builder: &IndexBuilder,
	ctx: &FrozenContext,
	opt: &Options,
	tb: TableId,
	ix: Arc<IndexDefinition>,
	blocking: bool,
) -> Result<()> {
	let rcv = index_builder.build(ctx, opt.clone(), tb, ix, blocking).await?;
	if let Some(rcv) = rcv {
		rcv.await.map_err(|_| DatastoreError::IndexingBuildingCancelled {
			reason: "Channel shutdown".to_string(),
		})?
	} else {
		Ok(())
	}
}

pub(crate) fn import_replay_can_reuse_index(
	ix: &IndexDefinition,
	table_name: &TableName,
	cols: &[Idiom],
	index: &Index,
) -> bool {
	!ix.prepare_remove
		&& ix.table_name.as_str() == table_name.as_str()
		&& ix.cols.as_slice() == cols
		&& ix.index == crate::catalog::Index::from(index.clone())
}
