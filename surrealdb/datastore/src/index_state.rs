//! Reading durable index build state.
//!
//! A concurrent `DEFINE INDEX` records its progress under `!bs` so that every
//! node reaches the same conclusion about an index. Two questions are asked of
//! that record from above: whether an index may serve a query, and what
//! `INFO FOR INDEX` should report. Both are reads over a transaction, so they
//! sit beside the keyspace that binds the record. The protocol that writes it —
//! admission, scanning, replay, takeover — needs the engine's document
//! machinery and stays one layer up.

use std::borrow::Cow;
use std::sync::Arc;

use anyhow::Result;
use surrealdb_kvs::key::{KVKeyDecode, RawRange};
use surrealdb_strand::Strand;

use crate::Transaction;
use crate::catalog::providers::TableProvider;
use crate::catalog::{
	DatabaseId, Index, IndexDefinition, IndexId, NamespaceId, guards_hnsw_pending,
};
#[cfg(diskann)]
use crate::key::schema::DiskannRecordPendingPrefix;
use crate::key::schema::{
	BuildStateKey, DocStatsBatchPrefix, DocStatsKey, HnswPendingLegacyPrefix,
	HnswRecordPendingPrefix, IndexCompactionIxPrefix, IndexCountKey, IndexCountPrefix,
	TermChangeBatchPrefix, TermChangesKey, TermDocsResidualPrefix,
};
use crate::pending_guard::{GuardedIndex, PENDING_GUARD_SHARDS, PendingGuardFamily};
use crate::val::{Object, TableName, Value};
use crate::values::diskann::{DiskAnnPendingState, DiskAnnPendingStateKind};
use crate::values::index_build::{IndexBuildPhase, IndexBuildReportStatus, IndexBuildState};

pub fn report_status_from_phase(phase: IndexBuildPhase) -> IndexBuildReportStatus {
	match phase {
		IndexBuildPhase::Building | IndexBuildPhase::Closing => IndexBuildReportStatus::Indexing,
		IndexBuildPhase::Online => IndexBuildReportStatus::Ready,
		IndexBuildPhase::Error => IndexBuildReportStatus::Error,
	}
}

pub fn durable_index_error_reason(ix: &IndexDefinition, state: &IndexBuildState) -> String {
	state.error.clone().unwrap_or_else(|| format!("Index {} is in an error state", ix.name))
}

/// Whether this index still has record updates waiting to be folded into it.
///
/// Every queued kind answers a read over its compacted state *plus* the updates
/// not yet folded into it, so a backlog never changes an answer, only what a
/// read costs. A vector index scores its unmaterialised updates by hand beside
/// the graph it walks, so a large enough set of them turns an indexed search
/// into something much closer to a scan. A full-text read folds its term and
/// statistics deltas, and any residual a compaction left, into the compacted
/// state it reads; a count sums every uncompacted delta.
///
/// Each kind is probed over exactly the ranges its compaction folds, and never
/// over the keys a compaction writes as its result: the `!td` bitmaps, the
/// `!dc` root and the `!iu` aggregate at the `None` bound are compacted state,
/// not a backlog. A full-text residual (`!tr`) is a backlog: it is what a
/// compaction left for a later round to fold, and is present until that
/// round. A b-tree kind writes final entries and has nothing to fold.
///
/// This reads the per-record pending entries themselves rather than the `!ic`
/// compaction-trigger queue. The trigger queue cannot answer the question: a
/// transaction buffers one trigger per index no matter how many records it
/// wrote, so its depth says nothing about outstanding work, and `index_compaction`
/// deletes trigger keys even where it declines to fold — during a build takeover,
/// for instance — so an empty trigger queue does not mean an empty index.
///
/// Existence, not depth: the first key is enough to answer it, and counting
/// would scan a range that after a bulk load is as long as the table. Callers
/// that need progress rather than a gate are better served by a count added
/// deliberately, with its cost understood.
///
/// # Arguments
/// * `tx` - the transaction the probe reads through.
/// * `ns`, `db` - the namespace and database the index's table is in.
/// * `ix` - the index to probe.
///
/// # Returns
/// Whether any update waits to be folded into the index.
pub async fn index_has_unmaterialised_updates(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	ix: &IndexDefinition,
) -> Result<bool> {
	let tb = Cow::Borrowed(&ix.table_name);
	// The ranges are separately typed, so they are probed in place rather than
	// collected, and each kind stops at the first range that holds a key.
	let unmaterialised = match ix.index {
		Index::FullText(_) => {
			let ix_id = ix.index_id;
			// The per-transaction deltas of both families, the per-document
			// deltas an upgraded index still carries beneath the `!tt` and `!dc`
			// bounds, and the residuals.
			let ranges: [RawRange; 5] = [
				TermChangeBatchPrefix::new(ns, db, tb.clone(), ix_id).range()?.into(),
				DocStatsBatchPrefix::new(ns, db, tb.clone(), ix_id).range()?.into(),
				TermChangesKey::new(ns, db, tb.clone(), ix_id).range()?.into(),
				DocStatsKey::new(ns, db, tb.clone(), ix_id).range()?.into(),
				TermDocsResidualPrefix::new(ns, db, tb, ix_id).range()?.into(),
			];
			let mut unmaterialised = false;
			for range in ranges {
				if !tx.keys_raw(range, 1, 0, None).await?.is_empty() {
					unmaterialised = true;
					break;
				}
			}
			unmaterialised
		}
		Index::Count(_) => {
			// The aggregate sits at the `None` bound, which sorts before every
			// delta, so the second key is the first delta if there is one.
			let range = IndexCountPrefix::new(ns, db, tb, ix.index_id).range()?;
			let keys = tx.keys(range, 2, 0, None).await?;
			let mut unmaterialised = false;
			for key in &keys {
				if IndexCountKey::decode_key(key)?.uid.is_some() {
					unmaterialised = true;
					break;
				}
			}
			unmaterialised
		}
		Index::Hnsw(_) if guards_hnsw_pending(ix.format_version) => {
			// Writes land in the sharded `!hw` range under the guards, as on
			// a DiskANN index, so the guards are the question. A `!hr` entry
			// written unguarded at the same stamp is under none, and lookup
			// scans that range too until compaction drains it.
			let guarded = GuardedIndex::new(
				PendingGuardFamily::Hnsw,
				ns,
				db,
				ix.table_name.clone(),
				ix.index_id,
			);
			if guards_non_empty(tx, &guarded).await? {
				true
			} else {
				let legacy = HnswRecordPendingPrefix::new(ns, db, tb, ix.index_id).range()?;
				!tx.keys(legacy, 1, 0, None).await?.is_empty()
			}
		}
		Index::Hnsw(_) => {
			let current = HnswRecordPendingPrefix::new(ns, db, tb.clone(), ix.index_id).range()?;
			if !tx.keys(current, 1, 0, None).await?.is_empty() {
				true
			} else {
				// Pre-`!hr` data still queues here and is still scored by hand.
				let legacy = HnswPendingLegacyPrefix::new(ns, db, tb, ix.index_id).range()?;
				!tx.keys(legacy, 1, 0, None).await?.is_empty()
			}
		}
		// DiskANN is compiled only for 64-bit non-wasm targets, and its pending
		// keyspace with it, so elsewhere this falls through to the arm below —
		// where there is no DiskANN index to ask about in the first place.
		#[cfg(diskann)]
		Index::DiskAnn(_) => {
			// Writes land in the sharded `!dw` range, and lookup decides per
			// shard whether to merge them by reading that shard's `!dy` guard.
			// So the guards are the question, not `!dw` itself.
			let guarded = GuardedIndex::new(
				PendingGuardFamily::DiskAnn,
				ns,
				db,
				ix.table_name.clone(),
				ix.index_id,
			);
			if guards_non_empty(tx, &guarded).await? {
				true
			} else {
				// Pre-shard data still lands in the unsharded range, which
				// lookup reads unconditionally alongside the shards.
				let legacy = DiskannRecordPendingPrefix::new(ns, db, tb, ix.index_id).range()?;
				!tx.keys(legacy, 1, 0, None).await?.is_empty()
			}
		}
		_ => false,
	};
	Ok(unmaterialised)
}

/// Whether any pending-state guard of `index` reads `NonEmpty`.
///
/// Only `NonEmpty` counts. A `MaybeEmpty` shard has already had its updates
/// folded in — the guard has just not been proved empty yet, so lookup still
/// scans a range it will find nothing in. Treating that as unsettled would
/// leave the field true on an index that answers at full speed, until some
/// later pass happened to close the guard. There are at most
/// [`PENDING_GUARD_SHARDS`] of these.
///
/// A shard this transaction wrote into counts as `NonEmpty` before its commit
/// arms the stored guard ([`Transaction::buffer_pending_guard`]).
async fn guards_non_empty(tx: &Transaction, index: &GuardedIndex) -> Result<bool> {
	if tx.buffered_pending_guard_shards(index) != 0 {
		return Ok(true);
	}
	Ok(armed_guard_shards(tx, index).await? > 0)
}

/// How many pending-state guard shards of `index` read `NonEmpty`, as
/// `INFO FOR INDEX` reports them in `building.armed_shards`.
///
/// A transaction that has buffered shards of its own counts those alone and
/// does not read the stored guards, so inside an uncommitted writer the count
/// is that writer's shards, not the stored ones: the index is known to be
/// pending, and a read of the guards would enter them into the read set of a
/// write transaction on a store that validates reads, so a writer arming
/// another shard would fail its commit. Otherwise the stored guards are
/// scanned, and the read is noted ([`Transaction::note_pending_guards_read`])
/// as every read of them is, so a write this transaction makes afterwards is
/// covered by its own commit rather than conflicting with the arming one.
async fn armed_guard_shards(tx: &Transaction, index: &GuardedIndex) -> Result<u32> {
	let mut armed = tx.buffered_pending_guard_shards(index);
	if armed != 0 {
		return Ok(armed.count_ones());
	}
	let guards: Vec<(Vec<u8>, DiskAnnPendingState)> =
		tx.scan(index.guards()?, PENDING_GUARD_SHARDS.into(), 0, None).await?;
	tx.note_pending_guards_read(index);
	for (raw, state) in guards {
		if state.kind == DiskAnnPendingStateKind::NonEmpty {
			let shard = index.decode_guard_shard(&raw)?;
			if shard < PENDING_GUARD_SHARDS {
				armed |= 1u64 << shard;
			}
		}
	}
	Ok(armed.count_ones())
}

/// Whether `ix` has unmaterialised updates and, for a guarded vector index,
/// how many of its pending-state guard shards read `NonEmpty`.
async fn index_unmaterialised_and_armed_shards(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	ix: &IndexDefinition,
) -> Result<(bool, u32)> {
	let tb = Cow::Borrowed(&ix.table_name);
	match ix.index {
		Index::Hnsw(_) if guards_hnsw_pending(ix.format_version) => {
			let guarded = GuardedIndex::new(
				PendingGuardFamily::Hnsw,
				ns,
				db,
				ix.table_name.clone(),
				ix.index_id,
			);
			let armed = armed_guard_shards(tx, &guarded).await?;
			if armed > 0 {
				Ok((true, armed))
			} else {
				let legacy = HnswRecordPendingPrefix::new(ns, db, tb, ix.index_id).range()?;
				Ok((!tx.keys(legacy, 1, 0, None).await?.is_empty(), 0))
			}
		}
		#[cfg(diskann)]
		Index::DiskAnn(_) => {
			let guarded = GuardedIndex::new(
				PendingGuardFamily::DiskAnn,
				ns,
				db,
				ix.table_name.clone(),
				ix.index_id,
			);
			let armed = armed_guard_shards(tx, &guarded).await?;
			if armed > 0 {
				Ok((true, armed))
			} else {
				let legacy = DiskannRecordPendingPrefix::new(ns, db, tb, ix.index_id).range()?;
				Ok((!tx.keys(legacy, 1, 0, None).await?.is_empty(), 0))
			}
		}
		_ => Ok((index_has_unmaterialised_updates(tx, ns, db, ix).await?, 0)),
	}
}

/// Whether `ix` has a compaction trigger buffered in `tx` or stored under
/// `/!ic`.
///
/// Unless `tx` buffered a trigger of its own, this scans the stored triggers
/// in `tx`, as the pending-prefix scans behind `compacting` scan theirs. Each
/// write to the index stores a trigger under its own key and a compaction
/// pass deletes them, so neither touches a key every writer shares, unlike
/// the pending-state guards [`armed_guard_shards`] keeps out of a writer's
/// read set. On a store that validates reads, a write transaction that runs
/// `INFO FOR INDEX` can still fail its commit when a writer or a pass changes
/// those keys before it commits, as it can for `compacting`.
async fn index_is_queued_for_compaction(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	ix: &IndexDefinition,
) -> Result<bool> {
	if !ix.index.queues_compaction() {
		return Ok(false);
	}
	if tx.has_buffered_compaction_trigger(ns, db, &ix.table_name, ix.index_id) {
		return Ok(true);
	}
	let range =
		IndexCompactionIxPrefix::new(ns, db, Cow::Borrowed(&ix.table_name), ix.index_id).range()?;
	Ok(!tx.keys(range, 1, 0, None).await?.is_empty())
}

fn index_building_status_value(
	ix: &IndexDefinition,
	state: Option<IndexBuildState>,
	unmaterialised: bool,
	active: bool,
	queued: bool,
	armed_shards: u32,
) -> Value {
	let Some(state) = state else {
		let mut out = Object::default();
		out.insert("active", active.into());
		out.insert("armed_shards", i64::from(armed_shards).into());
		out.insert("compacting", unmaterialised.into());
		out.insert("queued", queued.into());
		out.insert("status", IndexBuildReportStatus::Ready.as_str().into());
		return out.into();
	};
	let status = state.report_status.unwrap_or_else(|| report_status_from_phase(state.phase));
	let mut out = Object::default();
	if let Some(initial) = state.initial {
		out.insert("initial", initial.into());
	}
	if let Some(pending) = state.pending {
		out.insert("pending", pending.into());
	}
	if let Some(updated) = state.updated {
		out.insert("updated", updated.into());
	}
	if status == IndexBuildReportStatus::Error {
		out.insert("error", durable_index_error_reason(ix, &state).into());
	}
	out.insert("active", active.into());
	out.insert("armed_shards", i64::from(armed_shards).into());
	out.insert("compacting", unmaterialised.into());
	out.insert("queued", queued.into());
	out.insert("status", status.as_str().into());
	out.into()
}

/// Format `INFO FOR INDEX` output from durable build state.
///
/// Missing state is treated as ready so indexes created before this protocol
/// remain queryable and report the same shape as completed durable builds.
///
/// Every field but `active` is read from the store and answers alike on every
/// node. `active` is the caller's: whether a compaction pass over the index
/// runs in the caller's process, so a node not holding the compaction lease
/// reports `false` while another node compacts.
pub async fn index_building_info(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	ix: &IndexDefinition,
	active: bool,
) -> Result<Value> {
	let state_key = BuildStateKey {
		ns,
		db,
		tb: Cow::Borrowed(&ix.table_name),
		ix: ix.index_id,
	};
	let status = tx.get_key(&state_key, None).await?;
	let (unmaterialised, armed_shards) =
		index_unmaterialised_and_armed_shards(tx, ns, db, ix).await?;
	let queued = index_is_queued_for_compaction(tx, ns, db, ix).await?;
	let mut out = Object::default();
	out.insert(
		"building",
		index_building_status_value(ix, status, unmaterialised, active, queued, armed_shards),
	);
	Ok(out.into())
}

/// Check whether a possibly cached index definition is still catalog-reachable.
///
/// Missing durable state is ambiguous: it is expected for legacy indexes that
/// predate the durable build protocol, but it is also how retired indexes look
/// after their `!bs` key is deleted. The current catalog entry resolves that
/// ambiguity without adding work to the common durable-`Online` path.
pub async fn catalog_still_references_index(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table_name: &TableName,
	index_name: &Strand,
	index_id: IndexId,
) -> Result<bool> {
	Ok(matches!(
		catalog_index_state(tx, ns, db, table_name, index_name, index_id).await?,
		CatalogIndexState::Present
	))
}

/// What the catalog says about an index that a writer still holds a definition
/// for.
///
/// A holder that finds its index gone and one that finds it replaced owe the
/// write different things, so the two are named apart. Collapsing them loses
/// the distinction that decides whether a mutation may be skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogIndexState {
	/// The catalog names this exact index and it is not staged for removal.
	Present,
	/// The name is gone, or is staged for removal. Nothing maintains the index
	/// and nothing will replay a mutation queued against it.
	Retired,
	/// The name now belongs to a different index id. The replacement is not
	/// something a holder of the old id can maintain: its definition is not in
	/// that writer's snapshot, so the mutation would reach neither index.
	Replaced,
}

/// Resolve [`CatalogIndexState`] for one index.
///
/// Read this through a transaction that can actually observe the change being
/// ruled on. A user transaction cannot: its snapshot predates a concurrent
/// schema change, and [`crate::Transaction::get_tb_index`] answers from the
/// transaction cache before it consults storage.
pub async fn catalog_index_state(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table_name: &TableName,
	index_name: &Strand,
	index_id: IndexId,
) -> Result<CatalogIndexState> {
	let Some(current) = tx.get_tb_index(ns, db, &table_name.clone(), index_name, None).await?
	else {
		return Ok(CatalogIndexState::Retired);
	};
	if current.index_id != index_id {
		return Ok(CatalogIndexState::Replaced);
	}
	if current.prepare_remove {
		return Ok(CatalogIndexState::Retired);
	}
	Ok(CatalogIndexState::Present)
}

/// Remove indexes that have a durable build state but are not online.
///
/// Missing state is treated as online only when the catalog still references
/// the same index name and internal id. That preserves legacy/pre-durable
/// indexes while filtering stale execution-cache entries for retired indexes.
/// Build-state reads are batched so planner hot paths do not perform one
/// remote point read per index on distributed engines.
/// Document write paths intentionally read the full catalog so they can enqueue
/// updates for building indexes.
///
/// An index that a transaction of the same catalog epoch and build epoch read
/// as settled online ([`Transaction::index_known_online`]) is online without a
/// read, and the batch covers only the rest: none when none remains. An index
/// found settled online by a read here, with no build owner holding it or with
/// no build state while the catalog names it, is kept for those epochs
/// ([`Transaction::keep_index_online`]); no other state is.
pub async fn filter_online_indexes(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	indexes: Arc<[IndexDefinition]>,
) -> Result<Arc<[IndexDefinition]>> {
	if indexes.is_empty() {
		return Ok(indexes);
	}
	let from_known = tx.plans_from_known_online();
	let defined_unseen = tx.defined_unseen_indexes();
	let known_online = |ix: &IndexDefinition| {
		from_known
			&& !(defined_unseen && tx.index_unseen(ns, db, &ix.table_name, ix.index_id))
			&& tx.index_known_online(ns, db, &ix.table_name, ix.index_id)
	};
	// Every index known online answers without allocating; the set of those
	// that are not is built only once one is not.
	if indexes.iter().all(known_online) {
		return Ok(indexes);
	}
	let known: Vec<bool> = indexes.iter().map(known_online).collect();
	let state_keys: Vec<_> = indexes
		.iter()
		.zip(&known)
		.filter(|(_, known)| !**known)
		.map(|(ix, _)| BuildStateKey {
			ns,
			db,
			tb: Cow::Borrowed(&ix.table_name),
			ix: ix.index_id,
		})
		.collect();
	let mut states = tx.get_many_key(state_keys, None).await?.into_iter();
	let mut filtered = Vec::new();
	let mut filtered_any = false;
	for (ix, known) in indexes.iter().zip(&known) {
		if *known {
			filtered.push(ix.clone());
			continue;
		}
		let Some(state) = states.next() else {
			fail!("build state read answered fewer values than keys");
		};
		// An index the transaction cannot read answers nothing, whatever its
		// durable state says: the entries it holds are committed past the
		// transaction's snapshot, or not written yet, or lack a write of the
		// transaction's that was queued for a build or skipped. What is compiled
		// from this answer is right for this transaction alone, and no catalog
		// version names that, so nothing compiled from it is kept for another.
		let (online, settled) =
			if defined_unseen && tx.index_unseen(ns, db, &ix.table_name, ix.index_id) {
				tx.note_opaque_read();
				(false, false)
			} else {
				match state {
					Some(state) => (
						state.phase == IndexBuildPhase::Online,
						state.phase == IndexBuildPhase::Online && state.owner.is_none(),
					),
					None => {
						let present = catalog_still_references_index(
							tx,
							ns,
							db,
							&ix.table_name,
							&ix.name,
							ix.index_id,
						)
						.await?;
						(present, present)
					}
				}
			};
		if settled {
			tx.keep_index_online(ns, db, &ix.table_name, ix.index_id);
		}
		if online {
			filtered.push(ix.clone());
		} else {
			filtered_any = true;
		}
	}
	if filtered_any {
		Ok(Arc::from(filtered))
	} else {
		Ok(indexes)
	}
}
