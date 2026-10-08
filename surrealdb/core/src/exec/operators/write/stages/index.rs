//! The table's indexes.
//!
//! Index maintenance, and the doc-id release an in-flight index build may
//! defer.
//!
//! The two stages are deliberately separate and adjacent: the maintenance
//! reports the deferral on [`WriteRow::doc_id_removal_deferred`](super::super::row::WriteRow),
//! and the release acts on it before the purge. Folding them together, or
//! moving the release past the purge, breaks a concurrent build's replay.
//!
//! # What the index stage is handed
//!
//! Both images are read as they stand when the stage runs, which is *before*
//! the computed fields are materialised on every order that has both stages:
//! an index therefore holds the record's stored values. On `DELETE` the
//! post-image has already been emptied, so every entry the record held is
//! removed and nothing is written back.

use std::borrow::Cow;
use std::sync::Arc;

use reblessive::tree::TreeStack;

use crate::catalog::IndexDefinition;
use crate::dbs::{Force, Options};
use crate::err::EngineError;
use crate::exec::analyzer_function::StreamingAnalyzerFunction;
use crate::exec::operators::write::ctx::{CompiledIndexes, WriteCtx};
use crate::exec::operators::write::row::WriteRow;
use crate::exec::operators::write::stage::{StageId, StageOutcome, WriteStage};
use crate::exec::permission::{PhysicalIndexEntry, index_entry_physical};
use crate::exec::{BoxFut, Error as ExecError, ExecutionContext};
use crate::expr::ControlFlow;
use crate::idx::docids::TableDocIds;
use crate::idx::index::IndexOperation;
use crate::key::schema::DocPendingKey;
use crate::kvs::index::{ConsumeResult, IndexMutation};
use crate::val::{RecordId, RecordIdentity, Value};

/// This module's stages, by id.
///
/// `None` is a stage whose body has not landed:
/// [`WritePlan::resolve`](crate::exec::operators::write::plan::WritePlan)
/// refuses to bind a plan that names one, rather than running a partial order.
#[allow(
	clippy::wildcard_enum_match_arm,
	reason = "a stage this module does not own is not this module's to name, and `super::lookup` \
	          routes none of them here"
)]
pub(crate) fn stage(id: StageId) -> Option<Arc<dyn WriteStage>> {
	match id {
		StageId::StoreIndexData => Some(Arc::new(StoreIndexData)),
		StageId::ReclaimDocId => Some(Arc::new(ReclaimDocId)),
		// `super::lookup` routes only this module's stages here, so anything
		// else is a routing mistake rather than a missing body. Answering
		// `None` leaves it to `resolve` to name, as it names any unbound stage.
		_ => None,
	}
}

/// Bring the table's index entries in line with the record's two images.
///
/// Present in every order, once the post-image is final: after the record has
/// been written on a creating or updating order, and after it has been emptied
/// on `DELETE`. A record whose two images are equal is left alone unless the
/// session forced index maintenance. Each index is visited once — the values it
/// held for the before-image are removed and the values it holds for the
/// after-image are written — and an index the catalog has marked for removal is
/// skipped.
///
/// An index that is mid-build takes the mutation into the builder's queue
/// instead of applying it. For an index that consumes the table's shared
/// doc-id space that also defers the record's doc-id release to the build's
/// replay, which the stage reports on
/// [`WriteRow::doc_id_removal_deferred`] for [`ReclaimDocId`] to act on.
#[derive(Debug)]
struct StoreIndexData;

impl WriteStage for StoreIndexData {
	fn name(&self) -> &'static str {
		StageId::StoreIndexData.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			let deferred = maintain_indexes(row, ctx).await?;
			row.doc_id_removal_deferred = deferred;
			Ok(StageOutcome::Continue)
		})
	}
}

/// Release the record's entry in the table's shared doc-id space, or hand the
/// release to an in-flight build.
///
/// `DELETE` only, between [`StoreIndexData`] — which decides which of the two
/// this is — and the purge. The doc-id mapping is shared by every index on the
/// table, so it is dropped once, here, after every index has released the
/// record; dropping it while a build still owes this record a replay would
/// leave that replay unable to resolve the doc-id, so a durable marker is
/// written in this same transaction instead and the build's replay — or, if
/// the build never replays, the next build's sweep — completes the reclaim.
#[derive(Debug)]
struct ReclaimDocId;

impl WriteStage for ReclaimDocId {
	fn name(&self) -> &'static str {
		StageId::ReclaimDocId.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			let rid = record_id(row)?.clone();
			if row.doc_id_removal_deferred {
				defer_doc_id_removal(&rid, ctx).await?;
			} else {
				remove_doc_id(&rid, ctx).await?;
			}
			Ok(StageOutcome::Continue)
		})
	}
}

/// Apply the record's change to every index of the table, and answer whether a
/// doc-id index deferred the record's doc-id release to an in-flight build.
///
/// A table being dropped has its whole prefix reclaimed separately, so its
/// indexes are not maintained per record.
async fn maintain_indexes(row: &mut WriteRow, ctx: &WriteCtx) -> Result<bool, ControlFlow> {
	// A record whose images are equal has nothing to move in any index.
	// `OPTION FORCE = true` maintains the entries of every record the statement
	// writes, changed or not.
	if !forced(ctx) && !row.is_modified() {
		return Ok(false);
	}
	if ctx.table.drop {
		return Ok(false);
	}
	let rid = record_id(row)?.clone();
	if ctx.indexes.is_empty() {
		return Ok(false);
	}
	let compiled = compiled_indexes(ctx).await?;
	let exec = ctx.exec();
	let mut stack = TreeStack::new();
	let mut deferred = false;
	for (ix, entry) in ctx.indexes.iter().zip(compiled.iter()) {
		// Decommissioned indexes are ignored, and carry no compiled entry.
		let Some(entry) = entry else {
			continue;
		};
		let o = entry.values(&row.initial, exec).await.map_err(ControlFlow::Err)?;
		let n = entry.values(&row.current, exec).await.map_err(ControlFlow::Err)?;
		let count_cond_match = guard_match(entry, &row.initial, &row.current, exec).await?;
		// A regular index moves when the values it holds change. A COUNT WHERE
		// index holds no values — `o == n` is trivially true for it — and moves
		// when the predicate-match flag flips.
		let cond_changed = matches!(count_cond_match, Some((o, n)) if o != n);
		if o != n || cond_changed {
			deferred |= one_index(ctx, &mut stack, ix, o, n, &rid, count_cond_match).await?;
		}
	}
	Ok(deferred)
}

/// The table's index expressions, compiled on first use and once for the
/// statement; see [`WriteCtx::indexes_compiled`].
///
/// Parallel to `ctx.indexes` and in the same order, with `None` at each index
/// the catalog has marked for removal — those are never maintained, so their
/// clauses are never compiled.
///
/// The planner is built without a transaction, as the permission gate's is: a
/// planner that resolves no table context cannot recurse into an index
/// expression that names the table it indexes, and a stage running per record
/// shares no parent cycle guard that would otherwise break the recursion.
pub(crate) async fn compiled_indexes(ctx: &WriteCtx) -> Result<CompiledIndexes, ControlFlow> {
	{
		let cache = ctx.indexes_compiled.lock().await;
		if let Some(compiled) = cache.as_ref() {
			return Ok(Arc::clone(compiled));
		}
	}
	let exec = ctx.exec();
	let planner =
		crate::exec::vm::descriptor::write_unit_planner(exec.ctx(), exec.function_registry());
	let mut compiled = Vec::with_capacity(ctx.indexes.len());
	for ix in ctx.indexes.iter() {
		if ix.prepare_remove {
			compiled.push(None);
			continue;
		}
		let entry = index_entry_physical(ix, &planner)
			.await
			.map_err(|e| ControlFlow::Err(anyhow::Error::from(e)))?;
		compiled.push(Some(Arc::new(entry)));
	}
	let compiled: CompiledIndexes = compiled.into();
	let mut cache = ctx.indexes_compiled.lock().await;
	if let Some(published) = cache.as_ref() {
		return Ok(Arc::clone(published));
	}
	*cache = Some(Arc::clone(&compiled));
	Ok(compiled)
}

/// Whether each image satisfies a COUNT index's `WHERE` guard, as
/// `(before, after)`, and `None` for an index that has no guard.
pub(crate) async fn guard_match(
	entry: &PhysicalIndexEntry,
	initial: &Value,
	current: &Value,
	exec: &ExecutionContext,
) -> Result<Option<(bool, bool)>, ControlFlow> {
	let Some(old_matches) = entry.guard_matches(initial, exec).await.map_err(ControlFlow::Err)?
	else {
		return Ok(None);
	};
	let Some(new_matches) = entry.guard_matches(current, exec).await.map_err(ControlFlow::Err)?
	else {
		return Ok(None);
	};
	Ok(Some((old_matches, new_matches)))
}

/// Apply the record's change to one index.
///
/// Answers `true` when the index took the mutation into an in-flight build's
/// queue *and* consumes the table's shared doc-id space: the record's doc-id
/// must then outlive this transaction, because the build's replay still
/// resolves the record through it.
pub(crate) async fn one_index(
	ctx: &WriteCtx,
	stack: &mut TreeStack,
	ix: &IndexDefinition,
	o: Option<Vec<Value>>,
	n: Option<Vec<Value>>,
	rid: &RecordId,
	count_cond_match: Option<(bool, bool)>,
) -> Result<bool, ControlFlow> {
	let frozen = ctx.exec().ctx();
	// Does this index consume the table's shared doc-ID space?
	let doc_id_index = ix.uses_shared_doc_ids();
	let (o, n) = if let Some(ib) = frozen.get_index_builder() {
		let mutation = IndexMutation {
			old_values: o,
			new_values: n,
			rid,
			count_cond_match,
		};
		match ib.consume(&ctx.db, frozen, ix, mutation).await.map_err(ControlFlow::Err)? {
			// The build owns this mutation now and applies it when it replays,
			// so nothing is written here. For a doc-ID index that carries the
			// record's use of the shared `!di`/`!dd` mapping into the replay.
			ConsumeResult::Enqueued => return Ok(doc_id_index),
			// The build is finished; index the record normally.
			ConsumeResult::Ignored(o, n) => (o, n),
			// The definition was retired after it was read from a cache.
			ConsumeResult::Retired => return Ok(false),
		}
	} else {
		(o, n)
	};
	let az_fn = StreamingAnalyzerFunction::new(ctx.exec());
	let mut ic = IndexOperation::new(
		frozen,
		ctx.db.namespace_id,
		ctx.db.database_id,
		ctx.table.table_id,
		ix,
		o,
		n,
		rid,
	);
	if let Some((old_matches, new_matches)) = count_cond_match {
		ic = ic.with_count_cond_match(old_matches, new_matches);
	}
	// A compaction request raised while the entries are written is triggered
	// once the write is complete, never in the middle of it.
	let mut require_compaction = false;
	// The maintainer of a full-text index loads its analyzer as it computes
	// the entries.
	crate::exec::vm::descriptor::object(
		stack.enter(|stk| ic.compute(stk, &az_fn, &mut require_compaction)).finish(),
	)
	.await
	.map_err(ControlFlow::Err)?;
	if require_compaction {
		ic.trigger_compaction().await.map_err(ControlFlow::Err)?;
	}
	Ok(false)
}

/// Durably mark the record's shared doc-id mapping for a later reclaim,
/// because an in-flight index build enqueued this delete for replay and may
/// still need the mapping.
///
/// The marker is written in this same transaction — atomically with the
/// enqueued mutation — so the reclaim obligation exists from the moment the
/// central removal is skipped. If the build replays the delete, the replay
/// completes (or re-defers) the reclaim and consumes the marker; if the build
/// never replays it, the marker survives and the next doc-id index build's
/// sweep completes the reclaim instead.
async fn defer_doc_id_removal(rid: &RecordId, ctx: &WriteCtx) -> Result<(), ControlFlow> {
	let dp = DocPendingKey::new(
		ctx.db.namespace_id,
		ctx.db.database_id,
		Cow::Borrowed(&rid.table),
		RecordIdentity(rid.key.clone()),
	);
	ctx.txn().set_key(&dp, &()).await.map_err(ControlFlow::Err)
}

/// Remove the record's entry from the table's shared doc-id space.
///
/// The mapping (`!di`/`!dd`) is shared by every index on the table, so it is
/// dropped exactly once — here, after all per-index maintenance has released
/// the record — rather than by any individual index.
///
/// No-op for a table with no consumer of the shared space, so consumer-free
/// tables never allocate or write a mapping. The consumers are the doc-id
/// indexes (full-text / HNSW / DiskAnn / doc-ID-format b-tree) and — once the
/// table's `graph_doc_ids` marker is set — the numeric adjacency blocks
/// holding its record ids: a mapping surviving its record would hand a
/// re-created record its predecessor's id, which a block entry naming that id
/// would then resolve to.
async fn remove_doc_id(rid: &RecordId, ctx: &WriteCtx) -> Result<(), ControlFlow> {
	// A table being dropped has its whole prefix reclaimed separately.
	if ctx.table.drop {
		return Ok(());
	}
	let txn = ctx.txn();
	// Only tables with a consumer of the shared space maintain it.
	if !ctx.table.graph_doc_ids && !ctx.indexes.iter().any(IndexDefinition::uses_shared_doc_ids) {
		// The table definition is a plain snapshot read that registers no
		// read-conflict, so a concurrent fold committing the table's first
		// numeric block between that snapshot and this transaction's commit
		// would otherwise go undetected: the two transactions touch disjoint
		// write-sets, so no write-write check catches it either, even though
		// this read happened-before that write. Re-read the flag through the
		// transaction's settled read — locked, so commit-time OCC validates
		// it, and memoized, so a multi-record delete issues it once per table
		// — if a fold's flag-set is racing this transaction, one of the two is
		// forced to retry and the retry observes the settled value.
		let settled = txn
			.graph_doc_ids_settled(ctx.db.namespace_id, ctx.db.database_id, &ctx.table.name)
			.await
			.map_err(ControlFlow::Err)?;
		if !settled {
			return Ok(());
		}
	}
	TableDocIds::new(ctx.db.namespace_id, ctx.db.database_id, rid.table.clone())
		.remove(&txn, &rid.key)
		.await
		.map_err(ControlFlow::Err)?;
	Ok(())
}

/// Whether the session asked for index maintenance on every record it writes,
/// rather than only on the ones it changed.
fn forced(ctx: &WriteCtx) -> bool {
	matches!(ctx.exec().options(), Some(opt) if matches!(opt.force, Force::All))
}

/// The record the stages are maintaining entries for. Both stages run after
/// the record id exists on every order that names them.
fn record_id(row: &WriteRow) -> Result<&RecordId, ControlFlow> {
	row.id.as_ref().ok_or_else(|| {
		ControlFlow::Err(anyhow::anyhow!(EngineError::unreachable(
			"Expected a record id to be present"
		)))
	})
}

/// The evaluation environment an analyzer's `FUNCTION` clause is evaluated in:
/// the statement's own, passed through unchanged.
///
/// The index's own expressions — its columns and a `COUNT` index's guard — do
/// not read this: they are compiled and evaluated through the
/// engine, which reads its frame from the execution context.
fn options(ctx: &WriteCtx) -> Result<&Options, ControlFlow> {
	ctx.exec().options().ok_or_else(|| {
		ControlFlow::Err(anyhow::anyhow!(ExecError::Thrown(
			"Options not available to maintain the table's indexes".into()
		)))
	})
}

#[cfg(all(test, feature = "kv-mem"))]
#[expect(clippy::unwrap_used, reason = "a failed fixture is a broken test, not an assertion")]
mod tests {
	use std::borrow::Cow;

	use super::*;
	use crate::catalog::IndexId;
	use crate::exec::operators::test_util::{TestDb, val};
	use crate::exec::operators::write::clauses::WriteClauses;
	use crate::exec::operators::write::ctx::TableExistence;
	use crate::key::schema::IdxRoot;
	use crate::kvs::TransactionType;
	use crate::val::TableName;

	/// A write context over a fresh write transaction for table `t`. The
	/// context owns the transaction, so a stage's writes are readable through
	/// `ctx.txn()` without committing.
	async fn write_ctx(db: &TestDb) -> WriteCtx {
		let exec = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
		WriteCtx::resolve(
			&exec,
			&TableName::from("t"),
			TableExistence::Required,
			None,
			Arc::new(WriteClauses::default()),
		)
		.await
		.unwrap()
	}

	/// A row for `t:1` whose after-image differs from its before-image.
	async fn changed_row() -> WriteRow {
		let mut row = WriteRow::for_record(val("{ id: t:1, name: 'a' }").await).unwrap();
		row.current = val("{ id: t:1, name: 'b' }").await;
		row
	}

	/// How many keys the index holds, read through the write transaction, so
	/// the stage's own uncommitted writes are visible.
	async fn index_entries(ctx: &WriteCtx, ix: IndexId) -> usize {
		let key = IdxRoot {
			ns: ctx.db.namespace_id,
			db: ctx.db.database_id,
			tb: Cow::Borrowed(&ctx.table.name),
			ix,
		};
		let entries: Vec<(Vec<u8>, Vec<u8>)> = ctx.txn().get_prefix_key(&key, None).await.unwrap();
		entries.len()
	}

	/// Whether the record's deferred-reclaim marker is present.
	async fn pending_marker(ctx: &WriteCtx, id: &RecordId) -> bool {
		let dp = DocPendingKey::new(
			ctx.db.namespace_id,
			ctx.db.database_id,
			Cow::Borrowed(&id.table),
			RecordIdentity(id.key.clone()),
		);
		ctx.txn().get_key(&dp, None).await.unwrap().is_some()
	}

	/// The count delta this transaction holds for the index, which is where a
	/// COUNT index's mutations accumulate until the transaction commits.
	fn count_delta(ctx: &WriteCtx, ix: IndexId) -> i64 {
		ctx.txn().pending_count_delta(ctx.db.namespace_id, ctx.db.database_id, &ctx.table.name, ix)
	}

	/// The index of table `t`, whichever id the catalog gave it.
	fn only_index(ctx: &WriteCtx) -> IndexId {
		assert_eq!(ctx.indexes.len(), 1, "the fixture defines exactly one index");
		ctx.indexes[0].index_id
	}

	/// A record whose images differ has its entries written; one whose images
	/// are equal is left alone, which is what keeps an unchanged record from
	/// rewriting every index of its table.
	#[tokio::test]
	async fn only_a_changed_record_moves_its_entries() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS; DEFINE INDEX i ON t FIELDS name;").await;

		let ctx = write_ctx(&db).await;
		let ix = only_index(&ctx);
		let mut unchanged = WriteRow::for_record(val("{ id: t:1, name: 'a' }").await).unwrap();
		stage(StageId::StoreIndexData).unwrap().apply(&mut unchanged, &ctx).await.unwrap();
		assert_eq!(index_entries(&ctx, ix).await, 0, "an unchanged record writes no entry");

		let mut changed = changed_row().await;
		stage(StageId::StoreIndexData).unwrap().apply(&mut changed, &ctx).await.unwrap();
		assert_eq!(index_entries(&ctx, ix).await, 1, "a changed record writes its entry");
	}

	/// A table being dropped has its whole prefix reclaimed separately, so no
	/// per-record index work is done for it.
	#[tokio::test]
	async fn a_dropped_table_maintains_no_entries() {
		let db =
			TestDb::new("DEFINE TABLE t SCHEMALESS DROP; DEFINE INDEX i ON t FIELDS name;").await;
		let ctx = write_ctx(&db).await;
		let ix = only_index(&ctx);
		let mut row = changed_row().await;
		stage(StageId::StoreIndexData).unwrap().apply(&mut row, &ctx).await.unwrap();
		assert_eq!(index_entries(&ctx, ix).await, 0);
		assert!(!row.doc_id_removal_deferred, "nothing was enqueued, so nothing is deferred");
	}

	/// A COUNT index's `WHERE` guard is what decides membership, so the count
	/// moves when the guard's answer flips even though the index holds no
	/// column values to compare, and stays put when the answer does not change.
	#[tokio::test]
	async fn a_guarded_count_index_follows_its_guard() {
		let db =
			TestDb::new("DEFINE TABLE t SCHEMALESS; DEFINE INDEX i ON t COUNT WHERE name = 'b';")
				.await;
		let ctx = write_ctx(&db).await;
		let ix = only_index(&ctx);

		let mut accepted = changed_row().await;
		stage(StageId::StoreIndexData).unwrap().apply(&mut accepted, &ctx).await.unwrap();
		assert_eq!(count_delta(&ctx, ix), 1, "a record the guard newly accepts joins the count");

		let mut rejected = WriteRow::for_record(val("{ id: t:2, name: 'a' }").await).unwrap();
		rejected.current = val("{ id: t:2, name: 'c' }").await;
		stage(StageId::StoreIndexData).unwrap().apply(&mut rejected, &ctx).await.unwrap();
		assert_eq!(
			count_delta(&ctx, ix),
			1,
			"a record the guard rejects on both images leaves the count alone"
		);
	}

	/// The reclaim acts on what the index stage reported: a deferral writes the
	/// durable marker and leaves the mapping for the build's replay to release.
	#[tokio::test]
	async fn a_deferred_reclaim_writes_the_marker() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let ctx = write_ctx(&db).await;
		let mut row = changed_row().await;
		let id = row.id.clone().unwrap();
		row.doc_id_removal_deferred = true;
		stage(StageId::ReclaimDocId).unwrap().apply(&mut row, &ctx).await.unwrap();
		assert!(pending_marker(&ctx, &id).await, "a deferred release is marked durably");
	}

	/// Without a deferral the release happens now, and a table with no consumer
	/// of the shared doc-id space has nothing to release and no marker to
	/// leave behind.
	#[tokio::test]
	async fn an_undeferred_reclaim_leaves_no_marker() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS; DEFINE INDEX i ON t FIELDS name;").await;
		let ctx = write_ctx(&db).await;
		let mut row = changed_row().await;
		let id = row.id.clone().unwrap();
		assert!(!row.doc_id_removal_deferred);
		stage(StageId::ReclaimDocId).unwrap().apply(&mut row, &ctx).await.unwrap();
		assert!(!pending_marker(&ctx, &id).await, "an immediate release marks nothing");
	}

	/// Neither stage counts a record as affected: the affected-row total is the
	/// storage write's to set.
	#[tokio::test]
	async fn neither_stage_reports_a_mutation() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS; DEFINE INDEX i ON t FIELDS name;").await;
		let ctx = write_ctx(&db).await;
		let mut row = changed_row().await;
		for id in [StageId::StoreIndexData, StageId::ReclaimDocId] {
			let outcome = stage(id).unwrap().apply(&mut row, &ctx).await.unwrap();
			assert_eq!(outcome, StageOutcome::Continue, "{id:?} continues the order");
			assert!(!row.mutated, "{id:?} must not set the affected-row flag");
		}
	}
}
