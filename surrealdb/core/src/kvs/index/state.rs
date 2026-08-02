use std::borrow::Cow;
use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, Utc};
use surrealdb_strand::Strand;

use super::{BUILD_OWNER_LEASE_SECS, IndexBuildPhase, IndexBuildReportStatus, IndexBuildState};
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, IndexDefinition, IndexId, NamespaceId};
use crate::idx::IndexKeyBase;
use crate::key::schema::BuildStateKey;
use crate::kvs::{Error as KvsError, Transaction, storage_error};
use crate::val::{Object, TableName, Value};

pub(super) fn report_status_from_phase(phase: IndexBuildPhase) -> IndexBuildReportStatus {
	match phase {
		IndexBuildPhase::Building | IndexBuildPhase::Closing => IndexBuildReportStatus::Indexing,
		IndexBuildPhase::Online => IndexBuildReportStatus::Ready,
		IndexBuildPhase::Error => IndexBuildReportStatus::Error,
	}
}

pub(super) fn durable_index_error_reason(ix: &IndexDefinition, state: &IndexBuildState) -> String {
	state.error.clone().unwrap_or_else(|| format!("Index {} is in an error state", ix.name))
}

fn index_building_status_value(ix: &IndexDefinition, state: Option<IndexBuildState>) -> Value {
	let Some(state) = state else {
		let mut out = Object::default();
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
	out.insert("status", status.as_str().into());
	out.into()
}

/// Format `INFO FOR INDEX` output from durable build state.
///
/// Missing state is treated as ready so indexes created before this protocol
/// remain queryable and report the same shape as completed durable builds.
pub(crate) async fn index_building_info(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	ix: &IndexDefinition,
) -> Result<Value> {
	let ikb = IndexKeyBase::new(ns, db, ix.table_name.clone(), ix.index_id);
	let status = tx.get_key(&ikb.new_bs_key(), None).await?;
	let mut out = Object::default();
	out.insert("building", index_building_status_value(ix, status));
	Ok(out.into())
}

/// Delete durable build state for an index that is removed or overwritten.
///
/// The delete is staged in the caller's schema transaction so durable state and
/// queues disappear atomically with the catalog change that retires the index
/// definition. Once the catalog no longer references this `(name, IndexId)`,
/// missing `!bs` means retired state; while it still does, missing `!bs` is the
/// legacy/pre-durable ready state.
pub(crate) async fn retire_durable_index(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	ix: IndexId,
) -> Result<()> {
	let ikb = IndexKeyBase::new(ns, db, tb.clone(), ix);
	tx.del_key(&ikb.new_bs_key()).await?;
	delete_durable_build_queues(tx, &ikb).await?;
	Ok(())
}

/// Delete all generation-scoped durable queue keys for an index.
///
/// This is used when a fresh generation is published and when a schema change
/// retires the index. Takeover of an existing generation must not call this,
/// because it has to preserve the same-generation queued writes.
pub(super) async fn delete_durable_build_queues(
	tx: &Transaction,
	ikb: &IndexKeyBase,
) -> Result<()> {
	tx.delr(ikb.new_bg_all_generations_range()?).await?;
	tx.delr(ikb.new_bp_all_generations_range()?).await?;
	tx.delr(ikb.new_br_all_generations_range()?).await?;
	tx.delr(ikb.new_bt_all_generations_range()?).await?;
	Ok(())
}

/// Delete queued mutations, primary markers, and reservations for every
/// generation strictly below `below`.
///
/// Used by a new-generation takeover after the next generation's state has
/// been installed and the prior generations' reservations have drained: from
/// that point no writer can re-create entries under the old generations
/// (the flip removed the previous generation's `!bt` counter under a
/// conditional delete, and the admission fence rejects generation mismatches),
/// so the deletion is stable. Index retirement uses
/// [`delete_durable_build_queues`] instead, which clears every generation.
pub(super) async fn delete_stale_build_queues(
	tx: &Transaction,
	ikb: &IndexKeyBase,
	below: super::BuildGeneration,
) -> Result<()> {
	tx.delr(ikb.new_bg_range_below(below)?).await?;
	tx.delr(ikb.new_bp_range_below(below)?).await?;
	tx.delr(ikb.new_br_range_below(below)?).await?;
	// The flip that installed `below` already removed its immediate
	// predecessor's ticket counter under a conditional delete, which is what
	// fenced the writers still admitting to it. This sweeps up counters left
	// by generations further back, whose writers were fenced by their own
	// flips and can no longer allocate.
	tx.delr(ikb.new_bt_range_below(below)?).await?;
	Ok(())
}

pub(super) fn durable_report_count(count: Option<u64>) -> usize {
	match count {
		Some(count) => usize::try_from(count).unwrap_or(usize::MAX),
		None => 0,
	}
}

pub(super) fn is_condition_not_met(err: &anyhow::Error) -> bool {
	matches!(storage_error(err), Some(KvsError::TransactionConditionNotMet))
}

pub(super) fn build_owner_expired(state: &IndexBuildState, now: DateTime<Utc>) -> bool {
	state.owner_heartbeat_at.unwrap_or(state.updated_at)
		+ chrono::Duration::seconds(BUILD_OWNER_LEASE_SECS)
		<= now
}

/// Check whether a possibly cached index definition is still catalog-reachable.
///
/// Missing durable state is ambiguous: it is expected for legacy indexes that
/// predate the durable build protocol, but it is also how retired indexes look
/// after their `!bs` key is deleted. The current catalog entry resolves that
/// ambiguity without adding work to the common durable-`Online` path.
pub(super) async fn catalog_still_references_index(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table_name: &TableName,
	index_name: &Strand,
	index_id: IndexId,
) -> Result<bool> {
	let Some(current) = tx.get_tb_index(ns, db, &table_name.clone(), index_name, None).await?
	else {
		return Ok(false);
	};
	Ok(!current.prepare_remove && current.index_id == index_id)
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
pub(crate) async fn filter_online_indexes(
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
