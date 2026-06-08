use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, Utc};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{BUILD_OWNER_LEASE_SECS, BuildGeneration, BuildTicket};
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, IndexDefinition, IndexId, NamespaceId};
use crate::err::Error;
use crate::idx::IndexKeyBase;
use crate::key::table::bs::Bs;
use crate::kvs::{Error as KvsError, Transaction, impl_kv_value_revisioned};
use crate::val::{Object, TableName, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum IndexBuildReportStatus {
	/// Build state was created but no index-data cleanup has started yet.
	Started,
	/// Existing index data is being removed before the initial scan.
	Cleaning,
	/// The builder is scanning records, replaying queued writes, or closing.
	Indexing,
	/// The durable build phase is online and queries may use the index.
	Ready,
	/// The local builder was aborted before completion.
	Aborted,
	/// The durable build phase failed with an optional stored error reason.
	Error,
}

impl IndexBuildReportStatus {
	fn as_str(self) -> &'static str {
		match self {
			Self::Started => "started",
			Self::Cleaning => "cleaning",
			Self::Indexing => "indexing",
			Self::Ready => "ready",
			Self::Aborted => "aborted",
			Self::Error => "error",
		}
	}
}

/// Cluster-visible lifecycle for an index build generation.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum IndexBuildPhase {
	/// The builder is scanning records and writers may reserve tickets.
	Building,
	/// Initial indexing has completed and new writer admissions are blocked.
	Closing,
	/// The index has caught up with admitted writes and is queryable.
	Online,
	/// The build was aborted or failed; queries must not use the index.
	Error,
}

/// Durable per-index build state shared by all nodes.
///
/// The state is used as an admission counter for writers and as a fencing token
/// for the builder. Writers can update `updated_at` while allocating tickets;
/// only builders refresh `owner_heartbeat_at`, which controls lease expiry.
#[revisioned(revision = 3)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct IndexBuildState {
	/// Build epoch. Stale generation-scoped keys are ignored by newer builds.
	pub(crate) generation: BuildGeneration,
	/// Current durable lifecycle phase.
	pub(crate) phase: IndexBuildPhase,
	/// Concrete builder task that currently owns this generation.
	pub(crate) owner: Option<Uuid>,
	/// Next writer ticket to allocate while the build is in `Building`.
	pub(crate) next_ticket: BuildTicket,
	/// Whether initial record scanning has completed for this generation.
	pub(crate) initial_complete: bool,
	/// Last durable state update time.
	pub(crate) updated_at: DateTime<Utc>,
	/// Last builder-owned lease heartbeat.
	#[revision(start = 3)]
	pub(crate) owner_heartbeat_at: Option<DateTime<Utc>>,
	/// Durable error reason visible to every node once the build enters `Error`.
	#[revision(start = 2)]
	pub(crate) error: Option<String>,
	/// User-facing status for `INFO FOR INDEX`.
	#[revision(start = 3)]
	pub(crate) report_status: Option<IndexBuildReportStatus>,
	/// Number of records indexed during the initial scan.
	#[revision(start = 3)]
	pub(crate) initial: Option<u64>,
	/// Number of appended updates replayed after the initial scan.
	#[revision(start = 3)]
	pub(crate) updated: Option<u64>,
	/// Best-effort count of pending build updates visible to the builder.
	#[revision(start = 3)]
	pub(crate) pending: Option<u64>,
}

impl_kv_value_revisioned!(IndexBuildState);

/// Durable admission marker written before the user transaction commits.
///
/// The builder cannot move from `Closing` to `Online` until every reservation
/// for the generation has either been released after transaction close, produced
/// a durable appending that the builder can replay, or expired after its writer
/// node is no longer live.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct IndexBuildReservation {
	/// Node that reserved the ticket.
	pub(crate) node: Uuid,
	/// Deadline after which the reservation may be cleaned if the node is dead.
	pub(crate) expires_at: DateTime<Utc>,
}

impl_kv_value_revisioned!(IndexBuildReservation);

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
	let status = tx.get(&ikb.new_bs_key(), None).await?;
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
	tx.del(&ikb.new_bs_key()).await?;
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
	Ok(())
}

pub(super) fn durable_report_count(count: Option<u64>) -> usize {
	match count {
		Some(count) => usize::try_from(count).unwrap_or(usize::MAX),
		None => 0,
	}
}

pub(super) fn is_condition_not_met(err: &anyhow::Error) -> bool {
	if matches!(err.downcast_ref::<KvsError>(), Some(KvsError::TransactionConditionNotMet)) {
		return true;
	}
	matches!(err.downcast_ref::<Error>(), Some(Error::Kvs(KvsError::TransactionConditionNotMet)))
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
	ix: &IndexDefinition,
) -> Result<bool> {
	let Some(current) = tx.get_tb_index(ns, db, &ix.table_name, &ix.name, None).await? else {
		return Ok(false);
	};
	Ok(!current.prepare_remove && current.index_id == ix.index_id)
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
	let state_keys: Vec<_> =
		indexes.iter().map(|ix| Bs::new(ns, db, &ix.table_name, ix.index_id)).collect();
	let states = tx.getm(state_keys, None).await?;
	let mut filtered = Vec::new();
	let mut filtered_any = false;
	for (ix, state) in indexes.iter().zip(states) {
		let online = match state {
			Some(state) => state.phase == IndexBuildPhase::Online,
			None => catalog_still_references_index(tx, ns, db, ix).await?,
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
