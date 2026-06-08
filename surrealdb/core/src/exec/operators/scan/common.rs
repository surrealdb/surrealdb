//! Shared utilities for streaming execution operators.
//!
//! Contains helpers that are used by multiple scan operators (reference scan,
//! graph edge scan, etc.) to avoid code duplication.

use std::sync::Arc;

use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::exec::{ControlFlowExt, EvalContext, ExecutionContext, PhysicalExpr};
use crate::expr::ControlFlow;
use crate::kvs::{CachePolicy, Transaction};
use crate::val::{RecordId, RecordIdKey, Value};

/// Default value for [`crate::cnf::CommonConfig::scan_batch_size`] — the
/// number of records each scan operator buffers before yielding a batch
/// downstream. Read at runtime from config (overridable via
/// `SURREAL_SCAN_BATCH_SIZE`); this constant exists only as the
/// documentation anchor for that default.
pub(crate) const DEFAULT_SCAN_BATCH_SIZE: usize = 1000;

/// Convert a [`Value`] to a [`RecordIdKey`] for use in key range construction.
///
/// Used by operators that need to evaluate bound expressions and convert
/// the result into a key suitable for datastore range scans.
pub(crate) fn value_to_record_id_key(val: Value) -> RecordIdKey {
	match val {
		Value::Number(n) => RecordIdKey::Number(n.as_int()),
		Value::String(s) => RecordIdKey::String(s),
		Value::Uuid(u) => RecordIdKey::Uuid(u),
		Value::Array(a) => RecordIdKey::Array(a),
		Value::Object(o) => RecordIdKey::Object(o),
		// For other types, convert to string representation
		other => RecordIdKey::String(other.to_raw_string().into()),
	}
}

/// Extract [`RecordId`]s from a [`Value`] into an existing vec.
///
/// Handles single `RecordId` values, arrays of `RecordId`s, and Objects
/// by extracting the `id` field. The extracted `id` is recursively
/// processed, so objects whose `id` is an array of `RecordId`s (or a
/// nested object with its own `id`) are fully traversed, matching
/// SurrealQL semantics where graph traversal on an object uses its `id`.
pub(crate) fn extract_record_ids_into(val: Value, rids: &mut Vec<RecordId>) {
	match val {
		Value::RecordId(rid) => rids.push(rid),
		Value::Object(mut obj) => {
			if let Some(id_val) = obj.remove("id") {
				extract_record_ids_into(id_val, rids);
			}
		}
		Value::Array(arr) => {
			for v in arr {
				extract_record_ids_into(v, rids);
			}
		}
		_ => {}
	}
}

/// Evaluate a bound expression and convert the result to a [`RecordIdKey`].
///
/// Used by range-bounded scans to turn a `PhysicalExpr` bound value into a
/// key that can be encoded into datastore prefix/suffix bytes.
pub(crate) async fn evaluate_bound_key(
	expr: &Arc<dyn PhysicalExpr>,
	ctx: &ExecutionContext,
) -> Result<RecordIdKey, ControlFlow> {
	let eval_ctx = EvalContext::from_exec_ctx(ctx);
	let val = expr.evaluate(eval_ctx).await?;
	Ok(value_to_record_id_key(val))
}

/// Resolve the VERSION timestamp for a scan operator.
///
/// `version_expr` on a scan operator is the SELECT statement's VERSION
/// clause (propagated by the planner). The planner also wraps every
/// VERSION-bearing SELECT in a [`crate::exec::operators::version_scope::VersionScope`]
/// that evaluates the expression once in the *unversioned* outer
/// context and sets the resulting timestamp on
/// [`ExecutionContext::version_stamp`] before delegating to the inner
/// operator tree.
///
/// This helper prefers that already-resolved stamp and only falls back
/// to evaluating `version_expr` when no enclosing `VersionScope` was
/// inserted. Re-evaluating the expression inside the scan would run
/// under the just-set `version_stamp`, which can change how nested
/// lookups resolve. The motivating case: a `DEFINE PARAM $hist VALUE
/// time::now() PERMISSIONS FULL` followed by `SELECT … VERSION $hist`.
/// The param's storage revision is strictly after the `time::now()`
/// snapshot it stores, so `txn.get_db_param(..., version_stamp=$hist)`
/// from the re-evaluation can't find the param at that version, falls
/// back to `Value::None`, and the subsequent cast to `Datetime` fails
/// even though `VersionScope`'s unversioned evaluation already produced
/// the right timestamp.
pub(crate) async fn resolve_version_stamp(
	ctx: &ExecutionContext,
	version_expr: Option<&Arc<dyn PhysicalExpr>>,
) -> Result<Option<u64>, ControlFlow> {
	if let Some(stamp) = ctx.version_stamp() {
		return Ok(Some(stamp));
	}
	let Some(expr) = version_expr else {
		return Ok(None);
	};
	let eval_ctx = EvalContext::from_exec_ctx(ctx);
	let v = expr.evaluate(eval_ctx).await?;
	let stamp = v
		.cast_to::<crate::val::Datetime>()
		.map_err(|e| anyhow::anyhow!("{e}"))?
		.to_version_stamp(ctx.txn().timestamp_impl().as_ref())?;
	Ok(Some(stamp))
}

/// Resolve a batch of [`RecordId`]s into output values, applying each
/// record's table-level SELECT permission.
///
/// Records whose table permission denies them are skipped, so neither the
/// existence of the record nor its contents leak to a caller without view
/// access. Compiled permissions are cached in `perm_cache` keyed by table
/// name so that a single graph/reference scan over many edges only resolves
/// each table's permission once.
///
/// When `fetch_full` is `false` and `check_perms` is `false`, this avoids
/// fetching records and just wraps each id as `Value::RecordId`. Any other
/// combination requires reading the record so the permission predicate (if
/// any) can be evaluated against the actual data.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn resolve_record_batch(
	ctx: &ExecutionContext,
	txn: &Transaction,
	ns_id: NamespaceId,
	db_id: DatabaseId,
	rids: &[RecordId],
	fetch_full: bool,
	check_perms: bool,
	version: Option<u64>,
	cache_policy: CachePolicy,
	perm_cache: &mut std::collections::HashMap<
		crate::val::TableName,
		crate::exec::permission::PhysicalPermission,
	>,
) -> Result<Vec<Value>, ControlFlow> {
	use crate::exec::permission::{PhysicalPermission, check_permission_for_value};

	if !check_perms && !fetch_full {
		// Fast path: no permissions to check and no data to fetch. Wrap each
		// id as `Value::RecordId` directly.
		return Ok(rids.iter().map(|rid| Value::RecordId(rid.clone())).collect());
	}

	// Compile the SELECT permission for each distinct table referenced in
	// this batch. The cache survives across batches so we only resolve a
	// given table's permission once per scan.
	if check_perms {
		let db_ctx = ctx.database().context("permission resolution requires database context")?;
		for rid in rids {
			if perm_cache.contains_key(&rid.table) {
				continue;
			}
			let table_def = db_ctx
				.get_table_def(&rid.table, version)
				.await
				.context("Failed to get table definition")?;
			let catalog_perm =
				crate::exec::permission::resolve_select_permission(table_def.as_deref());
			let perm = crate::exec::permission::convert_permission_to_physical_runtime(
				catalog_perm,
				ctx.ctx(),
			)
			.await
			.context("Failed to convert permission")?;
			perm_cache.insert(rid.table.clone(), perm);
		}
	}

	let records = txn
		.get_records(ns_id, db_id, rids, version, cache_policy)
		.await
		.context("Failed to fetch records")?;

	let mut values = Vec::with_capacity(rids.len());
	for (rid, record) in rids.iter().zip(records) {
		// Missing records cannot disclose information.
		if record.data.is_none() {
			continue;
		}

		if check_perms {
			let perm = perm_cache.get(&rid.table).map_or(&PhysicalPermission::Deny, |p| p);
			let allowed = check_permission_for_value(perm, &record.data, None, ctx)
				.await
				.context("Failed to check permission")?;
			if !allowed {
				continue;
			}
		}

		if fetch_full {
			let value = match Arc::try_unwrap(record) {
				Ok(rec) => rec.data,
				Err(arc) => arc.data.clone(),
			};
			values.push(value);
		} else {
			values.push(Value::RecordId(rid.clone()));
		}
	}
	Ok(values)
}

/// Fetch full records for a batch of [`RecordId`]s in one batch, applying
/// permission filtering to each record.
///
/// Uses the transaction's batch multi-get (`get_records`), which is
/// cache-aware and uses the store's native batch read (e.g. RocksDB
/// `multi_get_opt`) for cache misses.  Records that don't exist or that
/// fail the permission check are silently skipped.
///
/// The record ID is already injected into the data by `get_records`, so
/// no additional `def()` call is needed.  When the `Arc<Record>` has a
/// reference count of 1, the data is moved out without cloning.
///
/// Used by [`super::index_scan::IndexScan`],
/// [`super::fulltext_scan::FullTextScan`], and
/// [`super::knn_scan::KnnScan`].
#[allow(clippy::too_many_arguments)]
pub(crate) async fn fetch_and_filter_records_batch(
	ctx: &ExecutionContext,
	txn: &Transaction,
	ns_id: NamespaceId,
	db_id: DatabaseId,
	rids: &[RecordId],
	select_permission: &crate::exec::permission::PhysicalPermission,
	check_perms: bool,
	version: Option<u64>,
	cache_policy: CachePolicy,
) -> Result<Vec<Value>, ControlFlow> {
	let records = txn
		.get_records(ns_id, db_id, rids, version, cache_policy)
		.await
		.context("Failed to fetch records")?;

	let mut values = Vec::with_capacity(rids.len());
	for record in records {
		if record.data.is_none() {
			continue;
		}

		if check_perms {
			// Permission checks need a reference; avoid moving data out of
			// the Arc until we know the record is allowed.
			let allowed = crate::exec::permission::check_permission_for_value(
				select_permission,
				&record.data,
				None,
				ctx,
			)
			.await
			.context("Failed to check permission")?;

			if !allowed {
				continue;
			}
		}

		// Move data out of the Arc when possible (refcount == 1),
		// otherwise fall back to cloning.
		let value = match Arc::try_unwrap(record) {
			Ok(rec) => rec.data,
			Err(arc) => arc.data.clone(),
		};
		values.push(value);
	}
	Ok(values)
}
