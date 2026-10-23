//! RecordLookup operator — record lookup and range scan by RecordId.
//!
//! Handles both single-record point lookups (`person:1`) and RecordId range
//! scans (`person:1..5`). Created by the planner when the FROM source is a
//! known RecordId (literal or parameter), and also used internally by
//! `DynamicScan` when it discovers a RecordId at runtime.

use std::sync::Arc;

use futures::StreamExt;

use super::pipeline::{
	FieldState, ScalarRows, ScanPipeline, build_field_state, filter_and_process_batch,
	kv_scan_stream,
};
use super::resolved::{EnforcementMode, planned_select_permission};
use crate::err::EngineError;
use crate::exec::operators::scan::count::record_key_range;
use crate::exec::permission::PhysicalPermission;
use crate::exec::{Clause, ExecutionContext};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::val::{RecordId, RecordIdKey, Value};

// =============================================================================
// Shared helper — used by both RecordLookup::execute and DynamicScan
// =============================================================================

/// Execute a record lookup or range scan for a resolved RecordId.
///
/// Handles both point lookups (non-range key) and range scans (range key).
/// Returns the resulting rows after applying permissions and computed fields.
///
/// The optional `predicate`, `limit`, and `start` parameters allow the caller
/// to push down WHERE / LIMIT / START clauses that would otherwise be handled
/// by outer pipeline operators. When `DynamicScan` discovers a RecordId at
/// runtime, it forwards its pushed-down values here so that the results are
/// correctly filtered and bounded.
///
/// `for_update` makes the point lookup a locked read: the record key is
/// registered on the storage engine for commit-time conflict detection,
/// including when the record does not exist. It is incompatible with a
/// `version` and with range keys — both are rejected here so a locked read
/// is never silently downgraded.
///
/// `mode` says who enforces what the table declares — the lookup per record
/// under [`EnforcementMode::Runtime`], the plan above under
/// [`EnforcementMode::Planned`].
///
/// This is the single implementation of RecordId-based data access, shared
/// by `RecordIdScan` (plan-time) and `DynamicScan` (runtime-discovered).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_record_lookup(
	rid: &RecordId,
	version: Option<u64>,
	for_update: bool,
	check_perms: bool,
	needed_fields: Option<&std::collections::HashSet<String>>,
	ctx: &ExecutionContext,
	predicate: Option<&Clause>,
	// FIXME: We should be consistent on which integer type we use for limit.
	// Our backend only support u32 so we should use u32 everywhere for limits.
	limit: Option<usize>,
	start: usize,
	mode: EnforcementMode,
) -> Result<Vec<Value>, ControlFlow> {
	let db_ctx = ctx.database().context("RecordLookup requires database context")?;
	let txn = ctx.txn();
	let ns = Arc::clone(&db_ctx.ns_ctx.ns);
	let db = Arc::clone(&db_ctx.db);

	if for_update {
		if version.is_some() {
			return Err(ControlFlow::Err(anyhow::Error::new(crate::exec::Error::Query {
				message: crate::dbs::FOR_UPDATE_VERSION_ERROR.to_string(),
			})));
		}
		if rid.key.is_range() {
			return Err(ControlFlow::Err(anyhow::Error::new(crate::exec::Error::Query {
				message: crate::dbs::FOR_UPDATE_TARGETS_ERROR.to_string(),
			})));
		}
	}

	// 1. Check table existence and resolve SELECT permission.
	let Some(table_def) =
		db_ctx.get_table_def(&rid.table, version).await.context("Failed to get table")?
	else {
		return Err(ControlFlow::Err(anyhow::Error::new(crate::catalog::Error::TbNotFound {
			name: rid.table.clone(),
		})));
	};

	let (select_permission, field_state) = match mode {
		// The plan above enforces.
		EnforcementMode::Planned => (
			if check_perms {
				planned_select_permission(&table_def)
			} else {
				PhysicalPermission::Allow
			},
			FieldState::empty(),
		),
		EnforcementMode::Runtime => {
			let perm = if check_perms {
				crate::exec::vm::descriptor::select_permission(ctx, &table_def.name).await?
			} else {
				PhysicalPermission::Allow
			};
			let fs = build_field_state(ctx, &rid.table, check_perms, needed_fields).await?;
			(perm, fs)
		}
	};

	// Early exit if denied
	if matches!(select_permission, PhysicalPermission::Deny) {
		return Ok(vec![]);
	}

	// A zero limit reads no record, except a
	// locked read: the lock is a side effect of the read whatever rows it
	// returns, so the point lookup below still registers it.
	if limit == Some(0) && !for_update {
		return Ok(vec![]);
	}

	// Pre-compute whether any post-decode processing is needed
	let needs_processing = ScanPipeline::compute_needs_processing(
		&select_permission,
		&field_state,
		check_perms,
		predicate,
	);

	// Row-filtering (permissions, WHERE) prevents positional pushdown;
	// row-modifying ops (computed fields, field perms) do not.
	let needs_row_filtering =
		ScanPipeline::compute_needs_row_filtering(&select_permission, predicate);

	// 3. Dispatch based on key type
	if let RecordIdKey::Range(range) = &rid.key {
		// --- Range scan ---
		let range = record_key_range(ns.namespace_id, db.database_id, &rid.table, range)?;

		let pre_skip = if !needs_row_filtering {
			start
		} else {
			0
		};
		let effective_storage_limit = if !needs_row_filtering {
			limit
		} else {
			None
		};
		let limit_hint = limit.map(|l| (l + start).try_into().unwrap_or(u32::MAX));

		let epoch = txn.layout_epoch();
		let layouts = txn
			.table_layouts(ns.namespace_id, db.database_id, &rid.table, version)
			.await
			.map_err(ControlFlow::Err)?;
		let gate = crate::kvs::RowGate::new(
			layouts,
			ns.namespace_id,
			db.database_id,
			&rid.table,
			version,
			epoch,
		)
		.map_err(ControlFlow::Err)?;
		let replay = crate::exec::migrate::replay_for(ctx, &rid.table, version).await?;
		let mut source = kv_scan_stream(
			Arc::clone(&txn),
			range,
			gate,
			version,
			effective_storage_limit,
			crate::kvs::Direction::Forward,
			pre_skip,
			limit_hint,
			None,
			// TopK threshold pushdown targets full table scans only.
			None,
			replay.clone(),
			ScalarRows::Content,
		);

		let mut pipeline = ScanPipeline::new(
			select_permission,
			predicate.cloned(),
			field_state,
			check_perms,
			limit,
			start.saturating_sub(pre_skip),
		);

		let mut results = Vec::new();
		while let Some(batch_result) = source.next().await {
			if ctx.cancellation().is_cancelled() {
				return Err(ControlFlow::Err(anyhow::anyhow!(EngineError::QueryCancelled)));
			}
			let mut batch = batch_result?;
			if let Some(replay) = &replay {
				replay.apply_batch(ctx, &mut batch).await?;
			}
			let cont = pipeline.process_batch(batch.values_mut(), ctx).await?;
			results.extend(batch.into_values());
			if !cont {
				break;
			}
		}
		Ok(results)
	} else {
		// --- Point lookup ---
		let fetched = if for_update {
			let fetched =
				crate::exec::operators::fetch::fetch_raw_record_for_update(ctx, rid).await?;
			if limit == Some(0) {
				return Ok(vec![]);
			}
			fetched
		} else {
			crate::exec::operators::fetch::fetch_raw_record(ctx, rid, version).await?
		};
		let Some(value) = fetched else {
			return Ok(vec![]);
		};

		let mut batch = vec![value];
		if needs_processing {
			filter_and_process_batch(
				&mut batch,
				&select_permission,
				predicate,
				ctx,
				&field_state,
				check_perms,
			)
			.await?;
		}
		// Apply start/limit for point lookups (0-1 rows after filtering).
		// Start skips the first N post-filter rows; with at most one row,
		// any non-zero start discards it.
		if !batch.is_empty() && start > 0 {
			batch.clear();
		}
		if !batch.is_empty() && limit == Some(0) {
			batch.clear();
		}
		Ok(batch)
	}
}
