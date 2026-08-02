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
use crate::catalog::{DatabaseId, IndexDefinition, IndexId, NamespaceId};
use crate::key::schema::BuildStateKey;
use crate::val::{Object, TableName, Value};
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
	let mut out = Object::default();
	out.insert("building", index_building_status_value(ix, status));
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
