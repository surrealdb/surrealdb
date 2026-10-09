//! A deleted record's storage, taken back out.
//!
//! One stage, and four things it does in this order: the record itself, the record's own inline
//! cache keys, the four graph keys an edge's endpoints hold, and the two cascades — over the
//! records joined to this one by an edge, and over the records that reference
//! it by a `REFERENCE` field.
//!
//! # Which of the row's two markers is read
//!
//! Every dispatch here reads
//! [`WriteRow::initial_metadata`](super::super::row::WriteRow), never
//! `metadata`. The keys being removed were written under the layout the record
//! *had*, and `ClearRecordData` — which runs earlier in `DELETE`'s order —
//! has already blanked the post-image and its marker. Reading `metadata` would
//! find `None`, take the latest-layout branch, and leave a legacy-layout edge's
//! four keys on disk with nothing left to name them.

use std::borrow::Cow;
use std::sync::Arc;

use surrealdb_datastore::values::graph::{AdjacencyValue, GraphFoldScope};
use surrealdb_types::ToSql;

use super::store::{endpoint, record_id, table_is_lightweight};
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, Error as CatalogError, FieldDefinition, NamespaceId};
use crate::doc::Error as DocError;
use crate::err::EngineError;
use crate::exe::FlowResultExt;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::metadata::{edge_variant, is_edge};
use crate::exec::operators::write::row::WriteRow;
use crate::exec::operators::write::scope::{
	NestedKey, ReferenceShape, WriteScope, planner_for, run_nested,
};
use crate::exec::operators::write::stage::{StageId, StageOutcome, WriteStage};
use crate::exec::permission::reference_custom_physical;
use crate::exec::{BoxFut, EvalContext};
use crate::expr;
use crate::expr::data::Assignment;
use crate::expr::logical::{LogicalOp, LogicalSubquery, lower_select};
use crate::expr::lookup::LookupKind;
use crate::expr::paths::{IN, OUT};
use crate::expr::reference::ReferenceDeleteStrategy;
use crate::expr::statements::{DeleteStatement, SelectStatement, UpdateStatement};
use crate::expr::{
	AssignOperator, ControlFlow, Data, Dir, Expr, Field, Fields, Idiom, Literal, Lookup, Param,
	Part,
};
use crate::idx::adjacency::{AdjacencyScope, MergedAdjacencyCursor, vertex_table_folded};
use crate::idx::inline_cache::{purge_record_caches, record_edge_delete};
use crate::key::KVKeyDecode;
use crate::key::schema::{
	GraphIdPrefix, GraphKey, GraphPointerKey, ReferenceIdPrefix, ReferenceKey,
};
use crate::kvs::{Direction, NORMAL_BATCH_SIZE, Transaction};
use crate::val::{RecordId, TableName, Value};

/// This module's stages, by id.
#[allow(
	clippy::wildcard_enum_match_arm,
	reason = "a stage this module does not own is not this module's to name, and `super::lookup` \
	          routes none of them here"
)]
pub(crate) fn stage(id: StageId) -> Option<Arc<dyn WriteStage>> {
	match id {
		StageId::PurgeRecordData => Some(Arc::new(PurgeRecordData)),
		// `super::lookup` routes only this module's stages here, so anything
		// else is a routing mistake rather than a missing body. Answering
		// `None` leaves it to `resolve` to name, as it names any unbound stage.
		_ => None,
	}
}

/// Take a deleted record's storage back out.
///
/// `DELETE` only, and late in its order: the reference cleanup has walked the
/// image, `ClearRecordData` has blanked it, the index maintenance has removed
/// the record's index entries and the doc-id reclaim has run. What is left is
/// the record's own bytes and the keys other records hold that name it.
///
/// # What it guarantees
///
/// - A record the statement did not change is left alone. On this order that means a record whose
///   before-image was already empty: `ClearRecordData` blanks the post-image, so every record that
///   existed is modified by the time this runs.
/// - The record's bytes are removed, and with them the inline cache keys its own table maintains.
///   The edge cascade below updates *other* records' caches; these are this record's.
/// - [`WriteRow::mutated`](super::super::row::WriteRow) is set once the deletion has completed, and
///   only then — the statement's affected-row total is that flag.
/// - An edge's four graph keys go with it, in the one layout the record was written in. A vertex
///   table with folded adjacency is tombstoned rather than key-deleted, because a packed block is
///   not reachable by key, and the fold that repays that debt is queued at commit rather than left
///   to the next reader.
#[derive(Debug)]
struct PurgeRecordData;

impl WriteStage for PurgeRecordData {
	fn name(&self) -> &'static str {
		StageId::PurgeRecordData.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			if !row.is_modified() {
				return Ok(StageOutcome::Continue);
			}
			let rid = record_id(row)?;
			// Everything read off the row is read here, before the first
			// await. A `WriteRow` holds a `OnceCell`, so a shared borrow of
			// one alive across an await would make this future — and with it
			// the whole write stream — `!Send`.
			let edge = is_edge(&row.initial_metadata)
				.then(|| {
					Ok::<_, ControlFlow>(Edge {
						left: endpoint(&row.initial, IN.as_ref(), "in")?,
						right: endpoint(&row.initial, OUT.as_ref(), "out")?,
						variant: edge_variant(&row.initial_metadata).unwrap_or_default(),
					})
				})
				.transpose()?;
			let ns = ctx.ns.namespace_id;
			let db = ctx.db.database_id;
			let txn = ctx.txn();

			txn.del_record(ns, db, &rid.table, &rid.key).await.map_err(ControlFlow::Err)?;
			purge_record_caches(&txn, ns, db, &rid).await.map_err(ControlFlow::Err)?;
			row.mutated = true;

			if let Some(edge) = edge {
				purge_pointers(ctx, &rid, &edge).await?;
			}
			let scope = ctx.scope()?;
			purge_edges(&scope, ctx, &rid).await?;
			purge_references(&scope, ctx, &rid).await?;
			Ok(StageOutcome::Continue)
		})
	}
}

/// Remove the four graph keys an edge record's endpoints hold.
///
/// The mirror of `StoreEdgesData`, and it must stay one: these are the keys
/// that stage wrote, and the two derive them from the same pieces.
///
/// ```text
///              ltr (target = r)         pointer
///          ┌─────────────────────┬─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
///          │                     ▼                  ▼
///     ┌────┴─────┐   etl  ┌────────────┐  etr   ┌──────────┐
///     │   left   │───────▶│ rid (edge) │───────▶│  right   │
///     └──────────┘   in   └────────────┘  out   └────┬─────┘
///           ▼                    ▼                   │
///           └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┴───────────────────┘
///                  pointer         rtl (source = l)
/// ```
///
/// `ltr` / `rtl` are the vertex-side keys, stored on the `in` and `out` vertex
/// with the opposite endpoint embedded so a traversal resolves the far vertex
/// without reading the edge. `etl` / `etr` are the edge-side keys, whose
/// adjacency already names the vertex, so they are the same in both layouts.
///
/// The vertex-side keys exist in exactly one of two layouts, and the record's
/// stored marker says which: a variant-1 record only ever wrote
/// [`GraphKey`]s, a later one only ever wrote [`GraphPointerKey`]s, because
/// `RELATE` supersedes the older form before writing the newer. Deleting only
/// the layout on disk halves the work against probing both.
async fn purge_pointers(ctx: &WriteCtx, rid: &RecordId, edge: &Edge) -> Result<(), ControlFlow> {
	let ns = ctx.ns.namespace_id;
	let db = ctx.db.database_id;
	let txn = ctx.txn();
	let Edge {
		left: l,
		right: r,
		variant,
	} = edge;

	let etl = GraphKey {
		ns,
		db,
		tb: Cow::Borrowed(&rid.table),
		id: Cow::Borrowed(&rid.key),
		dir: Dir::In,
		foreign_table: Cow::Borrowed(&l.table),
		foreign_key: Cow::Borrowed(&l.key),
	};
	let etr = GraphKey {
		ns,
		db,
		tb: Cow::Borrowed(&rid.table),
		id: Cow::Borrowed(&rid.key),
		dir: Dir::Out,
		foreign_table: Cow::Borrowed(&r.table),
		foreign_key: Cow::Borrowed(&r.key),
	};

	if *variant == 1 {
		let ltr = GraphKey {
			ns,
			db,
			tb: Cow::Borrowed(&l.table),
			id: Cow::Borrowed(&l.key),
			dir: Dir::Out,
			foreign_table: Cow::Borrowed(&rid.table),
			foreign_key: Cow::Borrowed(&rid.key),
		};
		let rtl = GraphKey {
			ns,
			db,
			tb: Cow::Borrowed(&r.table),
			id: Cow::Borrowed(&r.key),
			dir: Dir::In,
			foreign_table: Cow::Borrowed(&rid.table),
			foreign_key: Cow::Borrowed(&rid.key),
		};
		futures::try_join!(
			txn.del_key(&ltr),
			txn.del_key(&etl),
			txn.del_key(&etr),
			txn.del_key(&rtl)
		)
		.map_err(ControlFlow::Err)?;
		// A legacy-layout edge can never sit in a live inline cache — a
		// backfill that observes one spills — so these settle to no-ops. They
		// run for uniformity with the branch below.
		evict_from_caches(&txn, ns, db, l, r, rid).await?;
		return Ok(());
	}

	let ltr = pointer_key(ns, db, l, Dir::Out, rid, r);
	let rtl = pointer_key(ns, db, r, Dir::In, rid, l);
	// A vertex table with folded adjacency may hold this edge inside a packed
	// block, which no key deletion reaches: write a tombstone over the pointer
	// key instead, which the merged reader subtracts and the next fold
	// consumes. On a never-folded table the deletion is exact. Both writes are
	// blind — neither reads the pointer key first.
	let (l_folded, r_folded) = futures::try_join!(
		vertex_table_folded(&txn, ns, db, &l.table),
		vertex_table_folded(&txn, ns, db, &r.table)
	)
	.map_err(ControlFlow::Err)?;
	// A tombstone is debt the next fold repays; queue that fold at commit time
	// so it happens even if the scope is never read again.
	for (folded, vertex, dir) in [(l_folded, l, Dir::Out), (r_folded, r, Dir::In)] {
		if folded {
			txn.buffer_graph_fold_trigger(
				GraphFoldScope {
					ns,
					db,
					tb: vertex.table.clone(),
					id: vertex.key.clone(),
					dir,
				},
				ctx.exec().root().ctx.node_id(),
			);
		}
	}
	let tombstone = AdjacencyValue::tombstone();
	futures::try_join!(
		clear_pointer(&txn, &ltr, l_folded, &tombstone),
		clear_pointer(&txn, &rtl, r_folded, &tombstone)
	)?;
	// A lightweight edge never wrote edge-side keys.
	if !table_is_lightweight(&ctx.table) {
		futures::try_join!(txn.del_key(&etl), txn.del_key(&etr)).map_err(ControlFlow::Err)?;
	}
	evict_from_caches(&txn, ns, db, l, r, rid).await
}

/// The endpoints and layout of the edge being purged, read off the row's
/// before-image before any await.
struct Edge {
	/// The `in` vertex.
	left: RecordId,
	/// The `out` vertex.
	right: RecordId,
	/// The adjacency-key generation the record was stored under, which decides
	/// which of the two vertex-side layouts is on disk.
	variant: u16,
}

/// One vertex-side pointer key, removed the way the vertex's table allows: a
/// deletion where the table has never folded, and a tombstone where it has.
async fn clear_pointer(
	txn: &Arc<Transaction>,
	key: &GraphPointerKey<'_>,
	folded: bool,
	tombstone: &AdjacencyValue,
) -> Result<(), ControlFlow> {
	if folded {
		txn.set_key(key, tombstone).await.map_err(ControlFlow::Err)
	} else {
		txn.del_key(key).await.map_err(ControlFlow::Err)
	}
}

/// Drop the edge from each endpoint's inline adjacency cache.
///
/// The cache sits above the merged (blocks ∪ delta) tiers, so the removal is
/// the same whether the pointer key above was deleted or tombstoned.
async fn evict_from_caches(
	txn: &Arc<Transaction>,
	ns: NamespaceId,
	db: DatabaseId,
	l: &RecordId,
	r: &RecordId,
	rid: &RecordId,
) -> Result<(), ControlFlow> {
	record_edge_delete(txn, ns, db, l, Dir::Out, rid).await.map_err(ControlFlow::Err)?;
	record_edge_delete(txn, ns, db, r, Dir::In, rid).await.map_err(ControlFlow::Err)
}

/// One vertex-side pointer key: stored on `vertex` in `dir`, naming the edge,
/// with the opposite endpoint embedded.
fn pointer_key<'a>(
	ns: NamespaceId,
	db: DatabaseId,
	vertex: &'a RecordId,
	dir: Dir,
	edge: &'a RecordId,
	target: &'a RecordId,
) -> GraphPointerKey<'a> {
	GraphPointerKey {
		ns,
		db,
		tb: Cow::Borrowed(&vertex.table),
		id: Cow::Borrowed(&vertex.key),
		dir,
		foreign_table: Cow::Borrowed(&edge.table),
		foreign_key: Cow::Borrowed(&edge.key),
		target_table: Cow::Borrowed(&target.table),
		target_key: Cow::Borrowed(&target.key),
	}
}

// ===========================================================================
// The cascades
// ===========================================================================

/// `$__purge_vertex`: the record whose graph edges are being deleted.
pub(crate) const PURGE_VERTEX: &str = "__purge_vertex";
/// `$__purge_referencing`: the record holding a `REFERENCE` field that named
/// the record being deleted.
pub(crate) const PURGE_REFERENCING: &str = "__purge_referencing";
/// `$__purge_deleted`: the record being deleted, as an `ON DELETE UNSET`
/// repair of a collection subtracts it.
pub(crate) const PURGE_DELETED: &str = "__purge_deleted";
/// `$reference`: the record being deleted, as a custom delete strategy names
/// it. The name is the user-facing one the strategy is written against.
pub(crate) const REFERENCE: &str = "reference";

/// Delete the records joined to this one by a graph edge.
///
/// `DELETE $__purge_vertex<->`, run once for the record, with the vertex bound
/// per record rather than spliced in as a literal — one plan serves every
/// record the statement deletes.
///
/// # The peek
///
/// The adjacency is read for one key before the statement is compiled or run,
/// because a record with no edges is the common case and the cascade is not
/// free. It is read through the merged reader, which spans the per-edge
/// pointer keys and — once the table has folded — the packed blocks, and
/// subtracts tombstones, so a vertex whose edges are all deleted but not yet
/// folded cascades over nothing.
///
/// The peek cannot be replaced by a catalog check the way the reference scan
/// can. `RELATE` creates edges with no static schema flag, and a record in any
/// table — including a `TableType::Any` table never declared a relation — can
/// become an endpoint, so no table definition proves a record has no edges.
///
/// # The frame
///
/// The caller's own permission state, restored. An edge table's
/// `PERMISSIONS FOR delete` still applies when deleting one of its endpoints
/// triggers the cascade, or an actor holding vertex-delete permission could
/// erase edges it may not remove directly. That restoration is what
/// `caller_perms` is for, and it matters precisely when *this* delete was
/// itself reached through a `REFERENCE ON DELETE CASCADE`, whose bypass exists
/// to complete referential integrity and must not extend here.
pub(crate) async fn purge_edges(
	scope: &Arc<WriteScope>,
	ctx: &WriteCtx,
	rid: &RecordId,
) -> Result<(), ControlFlow> {
	if !has_edges(ctx, rid).await? {
		return Ok(());
	}

	let frame = ctx.exec().with_caller_perms().with_param(PURGE_VERTEX, Value::from(rid.clone()));
	let plan = scope
		.nested_plan(NestedKey::PurgeEdges, &frame, &[PURGE_VERTEX], purge_edges_statement)
		.await?;
	run_nested(&plan, &frame).await?;
	Ok(())
}

/// `DELETE $__purge_vertex<->`, as a plan.
pub(crate) fn purge_edges_statement() -> Result<LogicalOp, ControlFlow> {
	let what = Expr::Idiom(Idiom(vec![
		Part::Start(Expr::Param(Param::from(PURGE_VERTEX.to_owned()))),
		Part::Lookup(Box::new(Lookup {
			kind: LookupKind::Graph(Dir::Both),
			..Default::default()
		})),
	]));
	delete_of(what)
}

/// Whether the record `rid`, of the table `ctx` writes, has an edge to
/// cascade over.
///
/// A lightweight edge has none: nothing can connect to it, since it is
/// refused as a graph endpoint, and its synthesized adjacency derives from its
/// id whether or not it exists, so the peek would see its own endpoints.
pub(crate) async fn has_edges(ctx: &WriteCtx, rid: &RecordId) -> Result<bool, ControlFlow> {
	if table_is_lightweight(&ctx.table) {
		return Ok(false);
	}
	let ns = ctx.ns.namespace_id;
	let db = ctx.db.database_id;
	let txn = ctx.txn();
	let delta_range = GraphIdPrefix {
		ns,
		db,
		tb: Cow::Borrowed(&rid.table),
		id: Cow::Borrowed(&rid.key),
	}
	.range()
	.map_err(ControlFlow::Err)?;
	let mut cursor = MergedAdjacencyCursor::open(
		&txn,
		AdjacencyScope {
			ns,
			db,
			vertex: rid,
			dir: None,
			edge_table: None,
			fk_lower: None,
			delta_range,
		},
		ctx.table.graph_folded,
		None,
		Some(ctx.exec().root().ctx.get_index_stores().adjacency_resolve()),
	)
	.await
	.map_err(ControlFlow::Err)?;
	// Only emptiness matters, so one key is fetched and nothing is decoded.
	let any = !cursor.next_batch_keys(1).await.map_err(ControlFlow::Err)?.is_empty();
	drop(cursor);
	Ok(any)
}

/// Repair every record that referenced this one, as its `REFERENCE ON DELETE`
/// clause directs, and then remove the reference keys.
///
/// # The catalog gate
///
/// The scan is skipped outright when no `DEFINE FIELD … REFERENCE` in the
/// database can hold a record id of this table. A reference key under this
/// record's range is only ever written while such a field exists, and every
/// path that stops a field from targeting a table purges that table's
/// reference keys for it, so "no field can target this table" implies there is
/// nothing to find. It is a catalog read served from the transaction's cache,
/// so it costs no round-trip per record.
///
/// # Why the whole range is read before any repair
///
/// A cursor is a live view over the transaction's write batch on the
/// key-ordered backends, and a repair writes into that batch. Holding the
/// cursor across one would leave it reading invalidated state — type-legal,
/// silent, and wrong. The keys are therefore drained and the cursor dropped
/// before the first repair. The cost is holding one key per referencing
/// record; the alternative is a cursor that is only accidentally valid.
pub(crate) async fn purge_references(
	scope: &Arc<WriteScope>,
	ctx: &WriteCtx,
	rid: &RecordId,
) -> Result<(), ControlFlow> {
	#[cfg(test)]
	CASCADE_DISPATCHES.with(|runs| runs.set(runs.get() + 1));
	let Some((range, keys)) = referencing_keys(ctx, rid).await? else {
		return Ok(());
	};
	let ns = ctx.ns.namespace_id;
	let db = ctx.db.database_id;
	let txn = ctx.txn();

	// One field definition is read per distinct referencing field, not per key:
	// a record referenced many times over from one field is the common shape.
	let mut previous: Option<(TableName, String, Arc<FieldDefinition>)> = None;
	for key in keys {
		yield_now!();
		let ft = key.foreign_table.as_ref();
		let ff = key.foreign_field.as_ref();
		let field = match &previous {
			Some((table, field, definition)) if table == ft && field == ff => {
				Arc::clone(definition)
			}
			_ => {
				let definition = txn
					.get_tb_field(ns, db, ft, ff, None)
					.await
					.map_err(ControlFlow::Err)?
					.ok_or_else(|| {
						ControlFlow::Err(anyhow::anyhow!(CatalogError::FdNotFound {
							name: ff.to_string(),
						}))
					})?;
				previous = Some((ft.clone(), ff.to_owned(), Arc::clone(&definition)));
				definition
			}
		};
		let Some(reference) = &field.reference else {
			continue;
		};
		let referencing = RecordId {
			table: key.foreign_table.into_owned(),
			key: key.foreign_key.into_owned(),
		};
		repair(scope, ctx, rid, &referencing, &field, &reference.on_delete).await?;
	}
	txn.delr(range).await.map_err(ControlFlow::Err)
}

#[cfg(test)]
thread_local! {
	/// How many times this thread dispatched a purge's reference repairs through
	/// [`purge_references`], for tests that must know a delete dispatched them as
	/// instructions.
	pub(crate) static CASCADE_DISPATCHES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The reference keys naming `rid`, drained, and the range they were drained
/// from; `None` when no field in the database can target `rid`'s table, or
/// none names `rid`. See [`purge_references`] for why the whole range is read
/// before any repair.
pub(crate) async fn referencing_keys(
	ctx: &WriteCtx,
	rid: &RecordId,
) -> Result<Option<(surrealdb_kvs::key::TypedRange<()>, Vec<ReferenceKey<'static>>)>, ControlFlow> {
	let ns = ctx.ns.namespace_id;
	let db = ctx.db.database_id;
	let txn = ctx.txn();
	if !txn
		.table_may_have_incoming_references(ns, db, &rid.table)
		.await
		.map_err(ControlFlow::Err)?
	{
		return Ok(None);
	}
	let range = ReferenceIdPrefix {
		ns,
		db,
		tb: Cow::Borrowed(&rid.table),
		id: Cow::Borrowed(&rid.key),
	}
	.range()
	.map_err(ControlFlow::Err)?;
	let keys = drain_references(&txn, range.clone()).await?;
	// An empty range leaves nothing to repay, and the range delete would still
	// cost the transaction an op.
	if keys.is_empty() {
		return Ok(None);
	}
	Ok(Some((range, keys)))
}

/// Read a whole reference range into owned keys, with the cursor dropped
/// before the caller does anything else; see [`purge_references`].
async fn drain_references(
	txn: &Arc<Transaction>,
	range: surrealdb_kvs::key::TypedRange<()>,
) -> Result<Vec<ReferenceKey<'static>>, ControlFlow> {
	let mut cursor =
		txn.open_keys_cursor(range, Direction::Forward, 0, None).await.map_err(ControlFlow::Err)?;
	let mut keys = Vec::new();
	loop {
		let batch = cursor.next_batch(NORMAL_BATCH_SIZE).await.map_err(ControlFlow::Err)?;
		if batch.is_empty() {
			return Ok(keys);
		}
		for encoded in batch.iter() {
			keys.push(
				ReferenceKey::decode_key(encoded)
					.map(|key| key.into_owned())
					.map_err(ControlFlow::Err)?,
			);
		}
	}
}

/// Apply one `REFERENCE ON DELETE` clause to one referencing record.
///
/// A failure is reported as the referential repair it is, naming both records,
/// which is what makes an error from deep inside a cascade legible.
pub(crate) async fn repair(
	scope: &Arc<WriteScope>,
	ctx: &WriteCtx,
	rid: &RecordId,
	referencing: &RecordId,
	field: &FieldDefinition,
	strategy: &ReferenceDeleteStrategy,
) -> Result<(), ControlFlow> {
	match strategy {
		// The reference is left as it is: a dangling link is what the schema
		// asked for.
		ReferenceDeleteStrategy::Ignore => Ok(()),
		// The delete does not happen at all.
		ReferenceDeleteStrategy::Reject => Err(ControlFlow::Err(anyhow::anyhow!(
			DocError::DeleteRejectedByReference(rid.to_sql(), referencing.to_sql())
		))),
		ReferenceDeleteStrategy::Cascade => {
			cascade(scope, ctx, referencing).await.map_err(|err| wrap(rid, err))
		}
		ReferenceDeleteStrategy::Unset => {
			unset(scope, ctx, rid, referencing, field).await.map_err(|err| wrap(rid, err))
		}
		ReferenceDeleteStrategy::Custom(expr) => {
			custom(scope, ctx, rid, referencing, field, expr).await.map_err(|err| wrap(rid, err))
		}
	}
}

/// `ON DELETE CASCADE` — delete the referencing record.
///
/// The frame suppresses enforcement for that record while keeping the actor's
/// own state for whatever the delete goes on to trigger: the edge cascade
/// beneath it runs as the actor again.
pub(crate) async fn cascade(
	scope: &Arc<WriteScope>,
	ctx: &WriteCtx,
	referencing: &RecordId,
) -> Result<(), ControlFlow> {
	let frame = ctx
		.exec()
		.for_reference_cascade()
		.with_param(PURGE_REFERENCING, Value::from(referencing.clone()));
	let key = NestedKey::Reference {
		table: referencing.table.clone(),
		field: String::new(),
		shape: ReferenceShape::Cascade,
	};
	let plan = scope
		.nested_plan(key, &frame, &[PURGE_REFERENCING], || {
			delete_of(Expr::Param(Param::from(PURGE_REFERENCING.to_owned())))
		})
		.await?;
	run_nested(&plan, &frame).await.map(|_| ())
}

/// `ON DELETE UNSET` — take this record out of the referencing field.
///
/// Which statement that is depends on what the field holds, and the field's
/// declared kind does not settle it: a redefined kind leaves old rows behind.
/// So the referencing record is read and the shape decided per record. A field holding nothing
/// needs no repair.
pub(crate) async fn unset(
	scope: &Arc<WriteScope>,
	ctx: &WriteCtx,
	rid: &RecordId,
	referencing: &RecordId,
	field: &FieldDefinition,
) -> Result<(), ControlFlow> {
	let Some(row) = read_referencing(scope, ctx, referencing).await? else {
		return Ok(());
	};
	// Every shape but the three the strategy can repair is the same refusal:
	// the reference key names a field whose value cannot hold a link.
	#[allow(
		clippy::wildcard_enum_match_arm,
		reason = "a reference field holds a link, a collection of links, or nothing"
	)]
	let (shape, data) = match row.pick(&field.name) {
		Value::RecordId(_) => (ReferenceShape::Unset, unset_data(&field.name)),
		Value::Array(_) | Value::Set(_) => (ReferenceShape::Subtract, subtract_data(&field.name)),
		Value::None => return Ok(()),
		found => {
			return Err(ControlFlow::Err(anyhow::anyhow!(EngineError::unreachable(format!(
				"Expected either a record id, array, set or none, found {}",
				found.to_sql()
			)))));
		}
	};
	// The record being deleted is bound, as the referencing record is: one
	// plan serves every purge of the statement, and each removes its own link.
	let frame = ctx
		.exec()
		.with_perms_suppressed(true)
		.with_param(PURGE_REFERENCING, Value::from(referencing.clone()))
		.with_param(PURGE_DELETED, Value::from(rid.clone()));
	let key = NestedKey::Reference {
		table: referencing.table.clone(),
		field: field.name.to_sql(),
		shape,
	};
	let plan = scope
		.nested_plan(key, &frame, &[PURGE_REFERENCING, PURGE_DELETED], || {
			update_of(Expr::Param(Param::from(PURGE_REFERENCING.to_owned())), data)
		})
		.await?;
	run_nested(&plan, &frame).await.map(|_| ())
}

/// The data of the update that takes a link out of the single-link field
/// `field`: `UNSET field`.
pub(crate) fn unset_data(field: &Idiom) -> Data {
	Data::UnsetExpression(vec![field.clone()])
}

/// The data of the update that takes the deleted record out of the collection
/// field `field`: `SET field -= $__purge_deleted`.
pub(crate) fn subtract_data(field: &Idiom) -> Data {
	Data::SetExpression(vec![Assignment {
		place: field.clone(),
		operator: AssignOperator::Subtract,
		value: Expr::Param(Param::from(PURGE_DELETED.to_owned())),
	}])
}

/// `ON DELETE THEN <expr>` — the definition's own repair.
///
/// An expression rather than a statement, so it is compiled as a stored clause
/// and evaluated against the referencing record's *id*: the strategy is told
/// which record to repair and reads it itself. A `RETURN` out of it is the
/// strategy finishing, not a signal for the statement that triggered it.
async fn custom(
	scope: &Arc<WriteScope>,
	ctx: &WriteCtx,
	rid: &RecordId,
	referencing: &RecordId,
	field: &FieldDefinition,
	expr: &Expr,
) -> Result<(), ControlFlow> {
	let frame =
		ctx.exec().with_perms_suppressed(true).with_param(REFERENCE, Value::from(rid.clone()));
	// The expression is the field's own, so the field is part of what
	// identifies the compiled clause. Two fields of one referencing table
	// declaring different `THEN` expressions would otherwise share an entry,
	// and the second would run the first's repair.
	let key = NestedKey::Reference {
		table: referencing.table.clone(),
		field: field.name.to_sql(),
		shape: ReferenceShape::Custom,
	};
	let clause = scope
		.nested_clause(
			key,
			crate::exec::vm::descriptor::write_unit(async {
				let planner = planner_for(&frame)?;
				reference_custom_physical(expr, &planner)
					.await
					.map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))
			}),
		)
		.await?;
	let subject = Value::from(referencing.clone());
	let frame = frame.dive().map_err(ControlFlow::Err)?;
	clause
		.evaluate(EvalContext::from_exec_ctx(&frame).with_value_and_doc(&subject))
		.await
		.catch_return()
		.map(|_| ())
		.map_err(ControlFlow::Err)
}

/// The referencing record as the repair sees it: everything, with no
/// permission enforcement and with `COMPUTED` values derived.
async fn read_referencing(
	scope: &Arc<WriteScope>,
	ctx: &WriteCtx,
	referencing: &RecordId,
) -> Result<Option<Value>, ControlFlow> {
	let frame = ctx
		.exec()
		.with_perms_suppressed(true)
		.with_param(PURGE_REFERENCING, Value::from(referencing.clone()));
	let key = NestedKey::ReadReferencing {
		table: referencing.table.clone(),
	};
	let plan = scope
		.nested_plan(key, &frame, &[PURGE_REFERENCING], || {
			Ok(select_all_of(Expr::Param(Param::from(PURGE_REFERENCING.to_owned()))))
		})
		.await?;
	Ok(run_nested(&plan, &frame).await?.into_iter().next())
}

/// Report a failed repair as the referential failure it is, naming the record
/// whose deletion triggered it.
pub(crate) fn wrap(rid: &RecordId, err: ControlFlow) -> ControlFlow {
	#[allow(
		clippy::wildcard_enum_match_arm,
		reason = "a repair carries an error; every other signal passes through untouched"
	)]
	match err {
		ControlFlow::Err(err) => ControlFlow::Err(anyhow::anyhow!(DocError::RefsUpdateFailure(
			rid.to_sql(),
			err.to_string()
		))),
		signal => signal,
	}
}

// ===========================================================================
// The statements, built rather than parsed
// ===========================================================================

/// `DELETE <what>`, as a plan.
pub(crate) fn delete_of(what: Expr) -> Result<LogicalOp, ControlFlow> {
	let statement = DeleteStatement {
		what: vec![what],
		..Default::default()
	};
	lowered(expr::Expr::Delete(Box::new(statement)))
}

/// `UPDATE <what> <data>`, as a plan.
pub(crate) fn update_of(what: Expr, data: Data) -> Result<LogicalOp, ControlFlow> {
	let statement = UpdateStatement {
		what: vec![what],
		data: Some(data),
		..Default::default()
	};
	lowered(expr::Expr::Update(Box::new(statement)))
}

/// `SELECT * FROM <what>`, as a plan.
pub(crate) fn select_all_of(what: Expr) -> LogicalOp {
	let statement = SelectStatement {
		fields: Fields::Select(vec![Field::All]),
		what: vec![what],
		omit: vec![],
		only: false,
		with: None,
		cond: None,
		split: None,
		group: None,
		order: None,
		limit: None,
		start: None,
		fetch: None,
		version: Expr::Literal(Literal::None),
		timeout: Expr::Literal(Literal::None),
		explain: None,
		tempfiles: false,
		for_update: false,
	};
	LogicalOp::Subquery(LogicalSubquery {
		input: Box::new(
			lower_select(statement).expect("a SELECT of all fields has a logical form"),
		),
	})
}

/// One built statement, lowered.
///
/// The statements above are assembled as the engine's own AST and converted,
/// rather than formatted and re-parsed: a field name reaches them as the
/// catalog holds it, and nothing depends on that name surviving a round trip
/// through text.
fn lowered(statement: expr::Expr) -> Result<LogicalOp, ControlFlow> {
	crate::expr::logical::lower(statement).map_err(|err| ControlFlow::Err(anyhow::Error::new(err)))
}

#[cfg(all(test, feature = "kv-mem"))]
#[allow(clippy::unwrap_used)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::TestDb;
	use crate::exec::operators::write::clauses::WriteClauses;
	use crate::exec::operators::write::ctx::TableExistence;
	use crate::exec::operators::write::scope::WritePurpose;
	use crate::exec::operators::write::stage::WriteStage;
	use crate::kvs::TransactionType;
	use crate::val::RecordIdKey;

	/// A statement scope over a fresh write transaction, and the context for
	/// one table within it.
	async fn scoped(db: &TestDb, table: &str) -> (Arc<WriteScope>, Arc<WriteCtx>) {
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
		let scope = WriteScope::new(
			exec,
			Arc::new(WriteClauses::default()),
			None,
			TableExistence::Required,
		);
		let ctx =
			scope.write_ctx_for(&TableName::new(table), WritePurpose::Statement).await.unwrap();
		(scope, ctx)
	}

	fn id(table: &str, key: &str) -> RecordId {
		RecordId {
			table: TableName::new(table),
			key: RecordIdKey::String(key.into()),
		}
	}

	/// The row `DELETE`'s order carries by the time the purge runs: the record
	/// read from storage with its marker loaded, and the post-image blanked by
	/// `ClearRecordData`.
	async fn cleared(ctx: &WriteCtx, rid: &RecordId) -> WriteRow {
		let stored = ctx
			.txn()
			.get_record(ctx.ns.namespace_id, ctx.db.database_id, &rid.table, &rid.key, None)
			.await
			.unwrap();
		let mut row = WriteRow::for_record(stored.data.clone()).unwrap();
		row.id = Some(rid.clone());
		row.load_stored_metadata(ctx).await.unwrap();
		row.current = Value::None;
		row.metadata = None;
		row
	}

	/// How many adjacency keys one record carries, in both directions.
	async fn adjacency(ctx: &WriteCtx, record: &RecordId) -> usize {
		let range = GraphIdPrefix {
			ns: ctx.ns.namespace_id,
			db: ctx.db.database_id,
			tb: Cow::Borrowed(&record.table),
			id: Cow::Borrowed(&record.key),
		}
		.range()
		.unwrap();
		ctx.txn().keys_raw(range, 1000, 0, None).await.unwrap().len()
	}

	/// What storage holds for a record; a record that is not there answers
	/// `NONE`, as a missing key does.
	async fn stored(ctx: &WriteCtx, rid: &RecordId) -> Value {
		ctx.txn()
			.get_record(ctx.ns.namespace_id, ctx.db.database_id, &rid.table, &rid.key, None)
			.await
			.unwrap()
			.data
			.clone()
	}

	#[tokio::test]
	async fn a_purged_record_leaves_no_bytes_and_counts_as_affected() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS; CREATE t:a SET n = 1;").await;
		let (_scope, ctx) = scoped(&db, "t").await;
		let rid = id("t", "a");
		assert_ne!(stored(&ctx, &rid).await, Value::None, "the record exists to begin with");

		let mut row = cleared(&ctx, &rid).await;
		PurgeRecordData.apply(&mut row, &ctx).await.unwrap();

		assert_eq!(stored(&ctx, &rid).await, Value::None);
		assert!(row.mutated, "a record that reached storage counts towards the statement total");
	}

	/// A record the statement did not change is not purged, which is the guard
	/// the purge opens with. On `DELETE`'s order `ClearRecordData` runs
	/// first and blanks the post-image, so every record that existed reaches
	/// here modified; the guard is what keeps this stage from acting on an
	/// order that reached it without one.
	#[tokio::test]
	async fn an_unmodified_row_purges_nothing() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS; CREATE t:a SET n = 1;").await;
		let (_scope, ctx) = scoped(&db, "t").await;
		let rid = id("t", "a");

		// Read but not cleared: the post-image still equals the before-image.
		let mut row = cleared(&ctx, &rid).await;
		row.current = row.initial.clone();
		row.metadata.clone_from(&row.initial_metadata);
		assert!(!row.is_modified());

		PurgeRecordData.apply(&mut row, &ctx).await.unwrap();

		assert_ne!(stored(&ctx, &rid).await, Value::None, "the record is untouched");
		assert!(!row.mutated);
	}

	/// The four keys a `RELATE` wrote go with the edge record: one pointer key
	/// on each vertex, and one edge-side key per direction on the edge itself.
	#[tokio::test]
	async fn a_purged_edge_loses_all_four_graph_keys() {
		let db = TestDb::new(
			"DEFINE TABLE person SCHEMALESS; DEFINE TABLE likes TYPE RELATION SCHEMALESS;
			 CREATE person:a, person:b;
			 RELATE person:a->likes:one->person:b;",
		)
		.await;
		let (_scope, ctx) = scoped(&db, "likes").await;
		let edge = id("likes", "one");
		let (a, b) = (id("person", "a"), id("person", "b"));
		assert_eq!(adjacency(&ctx, &edge).await, 2, "the edge holds one key per direction");
		assert_eq!(adjacency(&ctx, &a).await, 1);
		assert_eq!(adjacency(&ctx, &b).await, 1);

		let mut row = cleared(&ctx, &edge).await;
		PurgeRecordData.apply(&mut row, &ctx).await.unwrap();

		assert_eq!(adjacency(&ctx, &edge).await, 0);
		assert_eq!(adjacency(&ctx, &a).await, 0, "the `in` vertex's pointer key goes too");
		assert_eq!(adjacency(&ctx, &b).await, 0, "and the `out` vertex's");
	}

	/// The dispatch reads the *before*-image's marker. `ClearRecordData` runs
	/// earlier in `DELETE`'s order and blanks the post-image marker, so a
	/// purge that asked the post-image would find no edge and leave all four
	/// keys behind with nothing left to name them.
	#[tokio::test]
	async fn the_edge_dispatch_reads_the_before_image_marker() {
		let db = TestDb::new(
			"DEFINE TABLE person SCHEMALESS; DEFINE TABLE likes TYPE RELATION SCHEMALESS;
			 CREATE person:a, person:b;
			 RELATE person:a->likes:one->person:b;",
		)
		.await;
		let (_scope, ctx) = scoped(&db, "likes").await;
		let edge = id("likes", "one");

		let row = cleared(&ctx, &edge).await;
		assert!(
			is_edge(&row.initial_metadata),
			"the before-image keeps the marker the purge dispatches on"
		);
		assert!(!is_edge(&row.metadata), "and the post-image has already lost it");
	}
}
