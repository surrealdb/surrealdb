use std::borrow::Cow;

use anyhow::Result;
use chrono::{DateTime, Utc};
pub(crate) use surrealdb_datastore::index_state::filter_online_indexes;
// Answering from durable build state needs only the keyspace that binds it, so
// those readers live with it; the protocol that writes the state stays here.
pub(super) use surrealdb_datastore::index_state::{
	CatalogIndexState, catalog_index_state, durable_index_error_reason, report_status_from_phase,
};

use super::{
	BUILD_OWNER_LEASE_SECS, IndexBuildBasis, IndexBuildPhase, IndexBuildReportStatus,
	IndexBuildState,
};
use crate::catalog::{AnalyzerDefinition, FromStored as _, IndexDefinition};
use crate::idx::IndexKeyBase;
use crate::key::KVKey;
use crate::key::schema::{AnalyzerKey, IdxRoot, IndexDefKey, IndexNameKey, TableKey};
use crate::kvs::{Error as KvsError, FeatureLevel, Release, Transaction, storage_error};

/// Whether analyzers `a` and `b` analyse the same way: the same function,
/// tokenizers and filters. Their names and comments do not count.
pub(super) fn analyzers_analyse_alike(a: &AnalyzerDefinition, b: &AnalyzerDefinition) -> bool {
	a.function == b.function && a.tokenizers == b.tokenizers && a.filters == b.filters
}

/// Whether the catalog, as `tx` reads it, describes what a build made against
/// `basis` wrote for the index `ikb` names: a definition of the index at
/// `basis`'s format version, and, where the build was handed an analyzer, an
/// analyzer of that name that analyses as that one does.
///
/// `None` where the catalog holds no definition of the index, or one staged
/// for removal: the index is being retired or was never committed, which the
/// paths that retire it settle ([`retire_uncommitted_build`]).
pub(super) async fn catalog_describes_basis(
	tx: &Transaction,
	ikb: &IndexKeyBase,
	basis: &IndexBuildBasis,
) -> Result<Option<bool>> {
	let (ns, db, tb) = (ikb.ns(), ikb.db(), ikb.table());
	let name = IndexNameKey {
		ns,
		db,
		tb: Cow::Borrowed(tb),
		ix: ikb.index(),
	};
	let Some(name) = tx.get_key(&name, None).await? else {
		return Ok(None);
	};
	let definition = IndexDefKey {
		ns,
		db,
		tb: Cow::Borrowed(tb),
		ix: Cow::Borrowed(name.as_str()),
	};
	let Some(stored) = tx.get_key(&definition, None).await? else {
		return Ok(None);
	};
	let definition = IndexDefinition::from_stored(&stored)?;
	if definition.prepare_remove {
		return Ok(None);
	}
	if definition.format_version != basis.format_version {
		return Ok(Some(false));
	}
	let Some(built) = &basis.analyzer else {
		return Ok(Some(true));
	};
	let key = AnalyzerKey {
		ns,
		db,
		az: Cow::Borrowed(built.name.as_str()),
	};
	let Some(stored) = tx.get_key(&key, None).await? else {
		return Ok(Some(false));
	};
	let committed = AnalyzerDefinition::from_stored(&stored)?;
	Ok(Some(analyzers_analyse_alike(&committed, built)))
}

/// Whether `state` names a generation built against a basis the catalog, as
/// `tx` reads it, does not describe ([`catalog_describes_basis`]). A
/// generation with no recorded basis is never stale.
pub(super) async fn build_basis_is_stale(
	tx: &Transaction,
	ikb: &IndexKeyBase,
	state: &IndexBuildState,
) -> Result<bool> {
	let Some(basis) = &state.basis else {
		return Ok(false);
	};
	Ok(catalog_describes_basis(tx, ikb, basis).await? == Some(false))
}

/// `state` withdrawn: its generation moved to `Error`, reported `aborted`,
/// with its owner cleared, so no query reads it and the resume scan leaves it.
fn withdrawn_build_state(state: &IndexBuildState) -> IndexBuildState {
	let mut withdrawn = state.clone();
	withdrawn.phase = IndexBuildPhase::Error;
	withdrawn.owner = None;
	withdrawn.error = None;
	withdrawn.report_status = Some(IndexBuildReportStatus::Aborted);
	withdrawn.updated_at = Utc::now();
	withdrawn.owner_heartbeat_at = None;
	withdrawn
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
		tx.clr_key(&ikb.new_bw_key()).await?;
		tx.clrr(ikb.new_bg_all_generations_range()?).await?;
		tx.clrr(ikb.new_bp_all_generations_range()?).await?;
		tx.clrr(ikb.new_br_all_generations_range()?).await?;
		tx.clrr(ikb.new_bt_all_generations_range()?).await?;
	} else {
		tx.del_key(&ikb.new_bs_key()).await?;
		tx.del_key(&ikb.new_bw_key()).await?;
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

/// Whether a builder may claim the build `state` names, as a takeover of its
/// generation: it names no owner, or its owner's lease has lapsed by `now`.
///
/// A `Building` or `Closing` state with no owner is one handed back for any
/// builder to continue ([`super::builder::Building::hand_back`]), or a
/// generation a committed `REBUILD INDEX` staged. It still fences compaction
/// ([`build_owner_holds`]), since its index is half built. Only a bounded pass
/// claims on this; every other claim waits out the lease
/// ([`build_owner_expired`]).
pub(super) fn build_claimable(state: &IndexBuildState, now: DateTime<Utc>) -> bool {
	state.owner.is_none() || build_owner_expired(state, now)
}

/// Who holds an index's build, as a compaction pass acts on it.
pub(crate) enum BuildOwner {
	/// No live owner: no build state, or one released, ended or failed.
	Released,
	/// A live owner, which fences the vector kinds.
	Held,
	/// An `Online` owner whose heartbeat lapsed, on a store where a lapsed
	/// owner may be cleared ([`lapsed_online_owners_clear`]), with the state
	/// that names it. It still fences until a transaction clears it with
	/// [`clear_lapsed_online_owner`].
	Lapsed(Box<IndexBuildState>),
}

/// Who holds the build of the index `ikb` names, read through `tx`: with a
/// locked read of `!bs` when `locked`, so a build acquiring or releasing the
/// index after the read conflicts the caller's commit, as a write that follows
/// the read needs.
pub(crate) async fn build_owner(
	tx: &Transaction,
	ikb: &IndexKeyBase,
	locked: bool,
) -> Result<BuildOwner> {
	let state = if locked {
		tx.getu_key(&ikb.new_bs_key()).await?
	} else {
		tx.get_key(&ikb.new_bs_key(), None).await?
	};
	build_owner_of(tx, state).await
}

/// Who holds the build whose state `state`, read from `!bs` through `tx`,
/// describes, as [`build_owner`] answers it, for a caller that has the state
/// already.
pub(crate) async fn build_owner_of(
	tx: &Transaction,
	state: Option<IndexBuildState>,
) -> Result<BuildOwner> {
	let Some(state) = state else {
		return Ok(BuildOwner::Released);
	};
	// A state that records its build's basis was written by a builder of this
	// release, which writes nothing of the index after `Online` and heartbeats
	// its ownership while its statement is open; a node of an earlier release
	// cannot decode it, let alone own it. Its lapse means that builder has
	// stopped, whatever the store's feature level.
	if online_owner_lapsed(&state, Utc::now())
		&& (state.basis.is_some() || lapsed_online_owners_clear(tx).await?)
	{
		return Ok(BuildOwner::Lapsed(Box::new(state)));
	}
	Ok(if build_owner_holds(Some(state)) {
		BuildOwner::Held
	} else {
		BuildOwner::Released
	})
}

/// Whether a builder anywhere in the cluster still owns the build `state`
/// names.
///
/// True while the state is `Building` or `Closing`, and while it is `Online`
/// with an owner that has not released it. A build writes nothing of the index
/// after `Online`, so that last window is only the wait for the statement that
/// started the build to close the transaction carrying the new definition —
/// until that commit lands, every other node reads the previous definition,
/// so this is the only signal that the build happened at all.
/// [`Building::defer_ownership_release`](super::builder::Building) is what
/// holds the window open that far.
///
/// Deliberately not bounded on the owner's heartbeat lease. Lease expiry is
/// what lets another builder *attempt* a takeover; it is not itself a fence,
/// because `maintain_build_ownership` accepts the same `(generation, owner)`
/// whatever the heartbeat age. A builder inside one long uninterruptible
/// stretch — an initial-scan or replay batch carries no internal checkpoint —
/// can let its heartbeat lapse and then carry on writing, and an `Online`
/// owner's statement can commit after its keeper stalled, so treating expiry
/// as "nobody owns this" would let maintenance run against a live writer or
/// through a definition that statement is replacing. Only a committed
/// compare-and-swap changes the owner, and this reads that: a takeover, or for
/// an `Online` owner, a clear that fences its statement in the same commit
/// ([`clear_lapsed_online_owner`]).
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

/// Whether a builder anywhere in the cluster still owns the build of the
/// index `ikb` names ([`build_owner_holds`]).
#[cfg(test)]
pub(crate) async fn build_owner_is_live(tx: &Transaction, ikb: &IndexKeyBase) -> Result<bool> {
	Ok(build_owner_holds(tx.get_key(&ikb.new_bs_key(), None).await?))
}

/// Whether `state` is an `Online` generation whose owner has let its heartbeat
/// lapse for a whole lease ([`build_owner_expired`]).
///
/// A lapse alone never ends the fence ([`build_owner_holds`]): the owner's
/// statement may still be open, and commit later. Only a transaction that
/// clears the owner and fences that statement in one commit acts on it
/// ([`clear_lapsed_online_owner`]), and only where
/// [`lapsed_online_owners_clear`] allows.
pub(crate) fn online_owner_lapsed(state: &IndexBuildState, now: DateTime<Utc>) -> bool {
	state.phase == IndexBuildPhase::Online
		&& state.owner.is_some()
		&& build_owner_expired(state, now)
}

/// Whether a lapsed `Online` owner may be cleared on the store `tx` reads:
/// only once its feature level is at least [`Release::FIRST_STORED`].
///
/// Clearing rests on every node that can own a build writing nothing of the
/// index after `Online`, and keeping its heartbeat while its statement is open.
/// A builder of an earlier release folds its own pendings after `Online`
/// without a heartbeat, so a lapse there does not mean it has stopped. The
/// feature level is the cluster-wide record that no such node runs: raising
/// it is refused while a running node records an older release or none. A
/// store held at [`FeatureLevel::Legacy`] keeps every `Online` owner fencing
/// until it releases or a `REBUILD INDEX` rewrites its build state, except one
/// whose state records its build's basis, which only a builder of this release
/// writes ([`build_owner_of`]).
pub(crate) async fn lapsed_online_owners_clear(tx: &Transaction) -> Result<bool> {
	Ok(matches!(
		tx.feature_level_state().await?.level,
		FeatureLevel::At(release) if release >= Release::FIRST_STORED
	))
}

/// Clear, in `tx`, the lapsed `Online` owner that `state`, read from `!bs`,
/// names, and fence the statement that owner's build may still be waiting on.
///
/// The owner is cleared by a compare-and-swap against `state`, so a heartbeat
/// or a release that lands first fails `tx`'s commit. The statement is fenced
/// through the table's definition: every `DEFINE INDEX` and `REBUILD INDEX`
/// takes a locked read of it and rewrites it, and so does this, unchanged. Of
/// `tx` and a statement still open on the index, whichever commits second
/// fails: a statement outlived by its lease can only roll back, and a
/// statement that commits first leaves `tx` to retry against the definition
/// it published. Clearing on expiry alone would let a compactor fold the
/// build's pendings through the definition that statement is replacing.
///
/// A generation whose recorded basis the catalog, as `tx` reads it, does not
/// describe is withdrawn instead of left `Online` ([`build_basis_is_stale`]):
/// its statement did not commit what the build was made against, and the
/// process that would have withdrawn it is gone. The fence makes that read
/// final, as a statement still open can no longer commit after `tx`.
///
/// # Returns
/// Whether it withdrew the generation. A caller owes a withdrawn generation no
/// fold and no compaction request: no query reads it, and the next build of
/// the index starts over.
pub(crate) async fn clear_lapsed_online_owner(
	tx: &Transaction,
	ikb: &IndexKeyBase,
	state: &IndexBuildState,
) -> Result<bool> {
	let stale = build_basis_is_stale(tx, ikb, state).await?;
	let cleared = if stale {
		withdrawn_build_state(state)
	} else {
		let mut cleared = state.clone();
		cleared.owner = None;
		cleared.updated_at = Utc::now();
		cleared
	};
	tx.put_compare_key(&ikb.new_bs_key(), &cleared, Some(state)).await?;
	fence_table_definition(tx, ikb).await?;
	Ok(stale)
}

/// Withdraw, in `tx`, the lapsed `Building` or `Closing` generation that
/// `state`, read from `!bs`, names, whose basis the catalog does not describe
/// ([`build_basis_is_stale`]), and fence the statement its build may still be
/// waiting on, as [`clear_lapsed_online_owner`] does.
///
/// The withdrawal is a compare-and-swap against `state`. The fence makes the
/// catalog read that found the basis stale final: of `tx` and a statement
/// still open on the index, whichever commits second fails, so a statement
/// that commits what the build was made against first leaves `tx` to retry
/// against it, and one still open can no longer commit after `tx`.
pub(super) async fn withdraw_stale_lapsed_build(
	tx: &Transaction,
	ikb: &IndexKeyBase,
	state: &IndexBuildState,
) -> Result<()> {
	tx.put_compare_key(&ikb.new_bs_key(), &withdrawn_build_state(state), Some(state)).await?;
	fence_table_definition(tx, ikb).await
}

/// Fence, in `tx`, every statement still open that defines or rebuilds an
/// index of the table `ikb` names.
///
/// Every `DEFINE INDEX` and `REBUILD INDEX` takes a locked read of the table's
/// definition and rewrites it. This takes a locked read of it too, and rewrites
/// it unchanged, or deletes it where the table is not defined yet (a table
/// defined by the open statement itself). Of `tx` and such a statement,
/// whichever commits second fails.
async fn fence_table_definition(tx: &Transaction, ikb: &IndexKeyBase) -> Result<()> {
	let table = TableKey {
		ns: ikb.ns(),
		db: ikb.db(),
		tb: Cow::Borrowed(ikb.table()),
	};
	match tx.getu_key_raw(&table).await? {
		Some(definition) => tx.set(table.encode_key()?, definition).await?,
		None => tx.del_key(&table).await?,
	}
	Ok(())
}

/// Whether `state`, the build state of an index the catalog does not define,
/// names an owner whose lease has lapsed by `now`: a build whose defining
/// statement never committed and that nothing heartbeats any more.
///
/// The resume scan visits only the indexes the catalog defines, so a
/// compaction pass that holds a request for such an index is the only reader
/// that reaches this state ([`retire_uncommitted_build`]).
pub(crate) fn uncommitted_build_lapsed(state: &IndexBuildState, now: DateTime<Utc>) -> bool {
	build_owner_holds(Some(state.clone())) && build_owner_expired(state, now)
}

/// Retire, in `tx`, the build of the index `ikb` names when its definition
/// never committed and its owner's lease has lapsed, and fence the statement
/// that defined it. Answers whether it retired.
///
/// Acts only where [`lapsed_online_owners_clear`] allows, when the index's
/// name lookup is absent under a locked read and its build state is one
/// [`uncommitted_build_lapsed`] accepts. It then:
/// - deletes the name lookup, and fences the table's definition ([`fence_table_definition`]), so
///   the statement can no longer commit;
/// - deletes the build state by a compare-and-delete against the state it read, so a heartbeat or a
///   batch that lands first fails `tx`'s commit;
/// - deletes the build's queues and the index's data.
///
/// A builder still running finds its state gone at its next ownership check,
/// which every transaction writing index data makes, and stops.
pub(crate) async fn retire_uncommitted_build(tx: &Transaction, ikb: &IndexKeyBase) -> Result<bool> {
	if !lapsed_online_owners_clear(tx).await? {
		return Ok(false);
	}
	let name = IndexNameKey {
		ns: ikb.ns(),
		db: ikb.db(),
		tb: Cow::Borrowed(ikb.table()),
		ix: ikb.index(),
	};
	if tx.getu_key(&name).await?.is_some() {
		return Ok(false);
	}
	let Some(state) = tx.getu_key(&ikb.new_bs_key()).await? else {
		return Ok(false);
	};
	if !uncommitted_build_lapsed(&state, Utc::now()) {
		return Ok(false);
	}
	tx.del_key(&name).await?;
	fence_table_definition(tx, ikb).await?;
	tx.del_compare_key(&ikb.new_bs_key(), Some(&state)).await?;
	delete_durable_build_queues(tx, ikb).await?;
	tx.delr(
		IdxRoot {
			ns: ikb.ns(),
			db: ikb.db(),
			tb: Cow::Borrowed(ikb.table()),
			ix: ikb.index(),
		}
		.range()?,
	)
	.await?;
	Ok(true)
}
