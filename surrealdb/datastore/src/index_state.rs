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
use surrealdb_strand::Strand;

use crate::Transaction;
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, Index, IndexDefinition, IndexId, NamespaceId};
use crate::key::schema::{BuildStateKey, HnswPendingLegacyPrefix, HnswRecordPendingPrefix};
#[cfg(diskann)]
use crate::key::schema::{DiskannPendingPrefix, DiskannRecordPendingPrefix};
use crate::val::{Object, TableName, Value};
#[cfg(diskann)]
use crate::values::diskann::{
	DISKANN_PENDING_STATE_SHARDS, DiskAnnPendingState, DiskAnnPendingStateKind,
};
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
/// A vector index answers a kNN search over its graph *plus* any updates not yet
/// materialised into that graph, and the unmaterialised ones have to be scored
/// by hand. A large enough set of them turns an indexed search into something
/// much closer to a scan: measured at ~600ms per query against ~5ms once the
/// index had absorbed them, on a 1M-row HNSW index.
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
async fn index_has_unmaterialised_updates(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	ix: &IndexDefinition,
) -> Result<bool> {
	let tb = Cow::Borrowed(&ix.table_name);
	// Only the graph indexes defer work this way; everything else is materialised
	// by the write that caused it. The ranges are separately typed, so they are
	// probed in place rather than collected — `||` also stops at the first hit.
	let unmaterialised = match ix.index {
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
			//
			// Only `NonEmpty` counts. The guards step `NonEmpty` ->
			// `MaybeEmpty` -> `Empty` across compaction passes, and a
			// `MaybeEmpty` shard has already had its updates folded in — the
			// guard has just not been proved empty yet, so lookup still scans
			// a range it will find nothing in. Treating that as unsettled
			// would leave the field true on an index that answers at full
			// speed, until some later pass happened to close the guard.
			// There are at most `DISKANN_PENDING_STATE_SHARDS` of these.
			let guards = DiskannPendingPrefix::new(ns, db, tb.clone(), ix.index_id).range()?;
			let guards: Vec<(Vec<u8>, DiskAnnPendingState)> =
				tx.scan(guards, DISKANN_PENDING_STATE_SHARDS.into(), 0, None).await?;
			if guards.iter().any(|(_, state)| state.kind == DiskAnnPendingStateKind::NonEmpty) {
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

fn index_building_status_value(
	ix: &IndexDefinition,
	state: Option<IndexBuildState>,
	unmaterialised: bool,
) -> Value {
	let Some(state) = state else {
		let mut out = Object::default();
		out.insert("status", IndexBuildReportStatus::Ready.as_str().into());
		out.insert("compacting", unmaterialised.into());
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
	out.insert("status", status.as_str().into());
	out.insert("compacting", unmaterialised.into());
	out.into()
}

/// Format `INFO FOR INDEX` output from durable build state.
///
/// Missing state is treated as ready so indexes created before this protocol
/// remain queryable and report the same shape as completed durable builds.
pub async fn index_building_info(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	ix: &IndexDefinition,
) -> Result<Value> {
	let state_key = BuildStateKey {
		ns,
		db,
		tb: Cow::Borrowed(&ix.table_name),
		ix: ix.index_id,
	};
	let status = tx.get_key(&state_key, None).await?;
	let unmaterialised = index_has_unmaterialised_updates(tx, ns, db, ix).await?;
	let mut out = Object::default();
	out.insert("building", index_building_status_value(ix, status, unmaterialised));
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
pub async fn filter_online_indexes(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	indexes: Arc<[IndexDefinition]>,
) -> Result<Arc<[IndexDefinition]>> {
	if indexes.is_empty() {
		return Ok(indexes);
	}
	let state_keys: Vec<_> = indexes
		.iter()
		.map(|ix| BuildStateKey {
			ns,
			db,
			tb: Cow::Owned(ix.table_name.clone()),
			ix: ix.index_id,
		})
		.collect();
	let states = tx.get_many_key(state_keys, None).await?;
	let mut filtered = Vec::new();
	let mut filtered_any = false;
	for (ix, state) in indexes.iter().zip(states) {
		let online = match state {
			Some(state) => state.phase == IndexBuildPhase::Online,
			None => {
				catalog_still_references_index(tx, ns, db, &ix.table_name, &ix.name, ix.index_id)
					.await?
			}
		};
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
