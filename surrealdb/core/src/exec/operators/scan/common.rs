//! Shared utilities for streaming execution operators.
//!
//! Contains helpers that are used by multiple scan operators (reference scan,
//! graph edge scan, etc.) to avoid code duplication.

use std::sync::Arc;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::exec::{ControlFlowExt, EvalContext, ExecutionContext, PhysicalExpr};
use crate::expr::ControlFlow;
use crate::kvs::Transaction;
use crate::val::{RecordId, RecordIdKey, Value};

/// Default batch size for collecting records before yielding downstream.
pub(crate) const BATCH_SIZE: usize = 1000;

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
		other => RecordIdKey::String(other.to_raw_string()),
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

/// Fetch full records for a batch of [`RecordId`]s in one batch.
///
/// Uses the transaction's batch multi-get (`getm_records`), which is cache-aware
/// and uses the store's native batch read (e.g. RocksDB `multi_get_opt`) for
/// cache misses.
///
/// The record ID is already injected into the data by `getm_records`, so no
/// additional `def()` call is needed here.  When the `Arc<Record>` has a
/// reference count of 1 (e.g. uncached / versioned reads), the data is moved
/// out without cloning.
///
/// Records that don't exist in the datastore are returned as [`Value::None`].
pub(crate) async fn fetch_records_batch(
	txn: &Transaction,
	ns_id: NamespaceId,
	db_id: DatabaseId,
	rids: &[RecordId],
	version: Option<u64>,
) -> Result<Vec<Value>, ControlFlow> {
	let records =
		txn.getm_records(ns_id, db_id, rids, version).await.context("Failed to fetch records")?;

	let mut values = Vec::with_capacity(rids.len());
	for record in records {
		if record.data.is_none() {
			values.push(Value::None);
		} else {
			// Move data out of the Arc when possible (refcount == 1),
			// otherwise fall back to cloning.
			let value = match Arc::try_unwrap(record) {
				Ok(rec) => rec.data,
				Err(arc) => arc.data.clone(),
			};
			values.push(value);
		}
	}
	Ok(values)
}

/// Resolve a batch of [`RecordId`]s into output values.
///
/// When `fetch_full` is false, wraps each ID as `Value::RecordId`.
/// When true, fetches all records concurrently via [`fetch_records_batch`].
pub(crate) async fn resolve_record_batch(
	txn: &Transaction,
	ns_id: NamespaceId,
	db_id: DatabaseId,
	rids: &[RecordId],
	fetch_full: bool,
	version: Option<u64>,
) -> Result<Vec<Value>, ControlFlow> {
	if fetch_full {
		fetch_records_batch(txn, ns_id, db_id, rids, version).await
	} else {
		Ok(rids.iter().map(|rid| Value::RecordId(rid.clone())).collect())
	}
}

/// Fetch full records for a batch of [`RecordId`]s in one batch, applying
/// permission filtering to each record.
///
/// Uses the transaction's batch multi-get (`getm_records`), which is
/// cache-aware and uses the store's native batch read (e.g. RocksDB
/// `multi_get_opt`) for cache misses.  Records that don't exist or that
/// fail the permission check are silently skipped.
///
/// The record ID is already injected into the data by `getm_records`, so
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
) -> Result<Vec<Value>, ControlFlow> {
	let records =
		txn.getm_records(ns_id, db_id, rids, version).await.context("Failed to fetch records")?;

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
