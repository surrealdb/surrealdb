use anyhow::Result;
use chrono::{DateTime, Utc};
// Answering from durable build state needs only the keyspace that binds it, so
// those readers live with it; the protocol that writes the state stays here.
pub(super) use surrealdb_datastore::index_state::{
	CatalogIndexState, catalog_index_state, durable_index_error_reason, report_status_from_phase,
};
pub(crate) use surrealdb_datastore::index_state::{filter_online_indexes, index_building_info};

use super::{BUILD_OWNER_LEASE_SECS, IndexBuildPhase, IndexBuildState};
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
///
/// Deleting `!bs` here is also the fence that stops the build. Every builder
/// transaction that writes index data brings `!bs` into its own conflict
/// domain first — a build batch by compare-and-swap, post-`Online` compaction
/// by locked read — so once this commits such a transaction can neither find
/// the state it needs nor commit against a deleted one. Aborting the
/// process-local task afterwards only saves it a failed batch.
pub(crate) async fn retire_durable_index(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	ix: IndexId,
	expunge: bool,
) -> Result<()> {
	let ikb = IndexKeyBase::new(ns, db, tb.clone(), ix);
	delete_durable_build_artifacts(tx, &ikb, expunge).await?;
	Ok(())
}

/// Delete build state and every generation-scoped queue for one index.
///
/// EXPUNGE must clear all MVCC versions, so a cascading removal passes its
/// policy down to the keys it retires here rather than leaving history a later
/// versioned read could still resolve.
pub(super) async fn delete_durable_build_artifacts(
	tx: &Transaction,
	ikb: &IndexKeyBase,
	expunge: bool,
) -> Result<()> {
	if expunge {
		tx.clr_key(&ikb.new_bs_key()).await?;
		tx.clrr(ikb.new_bg_all_generations_range()?).await?;
		tx.clrr(ikb.new_bp_all_generations_range()?).await?;
		tx.clrr(ikb.new_br_all_generations_range()?).await?;
		tx.clrr(ikb.new_bt_all_generations_range()?).await?;
	} else {
		tx.del_key(&ikb.new_bs_key()).await?;
		delete_durable_build_queues(tx, ikb).await?;
	}
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

/// Whether a builder anywhere in the cluster still owns this index.
///
/// True while the state is `Building` or `Closing`, and while it is `Online`
/// with an owner that has not released it. That last window covers both the
/// drain a build runs after publishing the index and the wait for the
/// statement that started it to close the transaction carrying the new
/// definition — until that commit lands, every other node reads the previous
/// definition, so this is the only signal that the build happened at all.
/// [`Building::defer_ownership_release`](super::builder::Building) is what
/// holds the window open that far.
///
/// Deliberately not bounded on the owner's heartbeat lease. Lease expiry is
/// what lets another builder *attempt* a takeover; it is not itself a fence,
/// because `maintain_build_ownership` accepts the same `(generation, owner)`
/// whatever the heartbeat age. A builder inside one long uninterruptible
/// stretch — a vector compaction plan carries no internal checkpoint — can let
/// its heartbeat lapse and then carry on writing, so treating expiry as
/// "nobody owns this" would let maintenance run against a live writer. Only a
/// committed CAS takeover changes the owner, and this reads that.
///
/// The cost of that choice is that a builder which dies mid-drain leaves the
/// index fenced until something rewrites `!bs`: the resume scan for a
/// `Building`/`Closing` generation, or the next `REBUILD INDEX` for a stranded
/// `Online` owner. That failure mode is bounded and benign — pendings simply
/// accumulate for a reader that already materialises them — where the
/// alternative is two writers folding the same pendings through different
/// on-disk layouts.
pub(crate) async fn build_owner_is_live(tx: &Transaction, ikb: &IndexKeyBase) -> Result<bool> {
	Ok(build_owner_holds(tx.get_key(&ikb.new_bs_key(), None).await?))
}

/// [`build_owner_is_live`] under a commit-time conflict registration.
///
/// For a caller that goes on to write in the same transaction. A build
/// acquiring or releasing ownership writes `!bs`, so registering the key here
/// means a build that starts after this read cannot commit alongside the
/// caller: one of the two is rejected, and the caller re-reads on retry.
/// Without it the read is only a snapshot, and a build acquired just after it
/// would be invisible to the write that follows.
pub(crate) async fn build_owner_is_live_locked(
	tx: &Transaction,
	ikb: &IndexKeyBase,
) -> Result<bool> {
	Ok(build_owner_holds(tx.getu_key(&ikb.new_bs_key()).await?))
}

fn build_owner_holds(state: Option<IndexBuildState>) -> bool {
	let Some(state) = state else {
		return false;
	};
	match state.phase {
		IndexBuildPhase::Building | IndexBuildPhase::Closing => true,
		IndexBuildPhase::Online => state.owner.is_some(),
		IndexBuildPhase::Error => false,
	}
}
