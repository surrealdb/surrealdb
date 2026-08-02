use anyhow::Result;
use chrono::{DateTime, Utc};
// Answering from durable build state needs only the keyspace that binds it, so
// those readers live with it; the protocol that writes the state stays here.
pub(super) use surrealdb_datastore::index_state::{
	catalog_still_references_index, durable_index_error_reason, report_status_from_phase,
};
pub(crate) use surrealdb_datastore::index_state::{filter_online_indexes, index_building_info};

use super::{BUILD_OWNER_LEASE_SECS, IndexBuildState};
use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::idx::IndexKeyBase;
use crate::kvs::{Error as KvsError, Transaction, storage_error};
use crate::val::TableName;

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
