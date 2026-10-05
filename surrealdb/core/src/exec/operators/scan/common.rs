//! Shared utilities for streaming execution operators.
//!
//! Contains helpers that are used by multiple scan operators (reference scan,
//! graph edge scan, etc.) to avoid code duplication.

use std::sync::Arc;

use super::pipeline::{FieldState, build_field_state, materialise_fields_with_permissions};
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId, table_select_permission};
use crate::exec::{ControlFlowExt, EvalContext, ExecutionContext, PhysicalExpr};
use crate::expr::{ControlFlow, FlowResult};
use crate::kvs::{CachePolicy, DatastoreError, Transaction};
use crate::val::{RecordId, RecordIdKey, TableName, Value};

/// Fail the query when the process is past `SURREAL_MEMORY_THRESHOLD`.
///
/// Scans that materialise records call this once per fetched batch, and
/// again once computed fields have been materialised into its rows or records
/// the transaction's cache shares have been cloned out of it, so a query that
/// keeps decoding records fails with
/// [`DatastoreError::QueryBeyondMemoryThreshold`] rather than growing the
/// process until the operating system kills it. Without allocation tracking
/// the threshold is never reached and this always returns `Ok`.
pub(crate) fn ensure_below_memory_threshold() -> FlowResult<()> {
	if crate::mem::ALLOC.is_beyond_threshold() {
		return Err(ControlFlow::Err(anyhow::Error::new(
			DatastoreError::QueryBeyondMemoryThreshold,
		)));
	}
	Ok(())
}

/// A batch of records as a scan fetched it.
pub(crate) struct FetchedBatch {
	/// The records that passed the permission check, in fetch order.
	pub(crate) values: Vec<Value>,
	/// The approximate decoded size of every record the fetch decoded,
	/// including those the permission check then dropped.
	pub(crate) fetched_bytes: usize,
	/// How many records the fetch decoded.
	pub(crate) fetched_rows: usize,
}

/// Decoded bytes a probe batch assumes per row, before any row size is known.
const PROBE_ROW_BYTES: usize = 64 << 10;

/// The number of rows in the first batch of a scan sized to `budget` bytes.
///
/// No row size is known yet, so the probe assumes rows of
/// `PROBE_ROW_BYTES`, about one 1024-float embedding decoded: a probe of such
/// rows stays within `budget`, and a smaller budget takes a smaller probe.
/// The result is never below 1 and never above `max_rows`.
pub(crate) fn probe_batch_len(budget: usize, max_rows: usize) -> usize {
	(budget / PROBE_ROW_BYTES).clamp(1, max_rows)
}

/// The number of rows the next batch may take so that a batch like the one
/// just fetched stays near `budget` bytes.
///
/// A row's size is the average of `fetched_bytes` over `fetched_rows`: what
/// the fetch held, rows a permission check then dropped included, so a batch
/// whose survivors are narrow is not taken for a batch of narrow records.
///
/// The prediction describes the batch just fetched, not the next one, so the
/// next batch is at most twice `current`, the length that batch was fetched
/// with: a run of narrow records grows the batch gradually, and wide records
/// that follow it are decoded in a batch no larger than twice the last one
/// before the size shrinks again. The result is never below 1 and never above
/// `max_rows`, so a batch never grows past the row cap it would have without a
/// byte budget. A fetch that decoded no record says nothing about row size, so
/// it keeps `current`.
pub(crate) fn next_batch_len(
	fetched_bytes: usize,
	fetched_rows: usize,
	current: usize,
	budget: usize,
	max_rows: usize,
) -> usize {
	if fetched_rows == 0 {
		return current;
	}
	let per_row = (fetched_bytes / fetched_rows).max(1);
	(budget / per_row).min(current.saturating_mul(2)).clamp(1, max_rows)
}

/// Approximate in-memory size of a decoded [`Value`], in bytes.
///
/// Counts one inline `Value` per node, plus the bytes of strings and byte
/// buffers, a key slot and the key's bytes per object entry, a coordinate per
/// geometry point, and the contents of record-id keys and ranges; any other
/// variant counts only its inline size. It sizes
/// batches, so it trades precision for a single pass with no allocation.
/// Recursion is bounded by the value nesting limit enforced at decode.
pub(crate) fn approx_value_size(value: &Value) -> usize {
	let inline = std::mem::size_of::<Value>();
	match value {
		Value::String(s) => inline + s.len(),
		Value::Bytes(b) => inline + b.0.len(),
		Value::Array(a) => inline + a.iter().map(approx_value_size).sum::<usize>(),
		Value::Set(s) => inline + s.0.iter().map(approx_value_size).sum::<usize>(),
		Value::Object(o) => {
			let entry = std::mem::size_of::<surrealdb_strand::Strand>();
			inline + o.iter().map(|(k, v)| entry + k.len() + approx_value_size(v)).sum::<usize>()
		}
		Value::Geometry(g) => inline + geometry_heap_size(g),
		Value::RecordId(rid) => inline + rid.table.as_str().len() + approx_key_size(&rid.key),
		Value::Range(r) => {
			inline
				+ std::mem::size_of::<crate::val::Range>()
				+ approx_bound_size(&r.start, approx_value_size)
				+ approx_bound_size(&r.end, approx_value_size)
		}
		_ => inline,
	}
}

/// The owned contents of a record-id key, beyond its inline size.
fn approx_key_size(key: &RecordIdKey) -> usize {
	match key {
		RecordIdKey::Number(_) | RecordIdKey::Uuid(_) => 0,
		RecordIdKey::String(s) => s.len(),
		RecordIdKey::Array(a) => a.iter().map(approx_value_size).sum(),
		RecordIdKey::Object(o) => o
			.iter()
			.map(|(k, v)| {
				std::mem::size_of::<surrealdb_strand::Strand>() + k.len() + approx_value_size(v)
			})
			.sum(),
		RecordIdKey::Range(r) => {
			std::mem::size_of::<crate::val::RecordIdKeyRange>()
				+ approx_bound_size(&r.start, approx_key_size)
				+ approx_bound_size(&r.end, approx_key_size)
		}
	}
}

/// The size of a range bound's value, by `size`; 0 for an unbounded end.
fn approx_bound_size<T>(bound: &std::ops::Bound<T>, size: fn(&T) -> usize) -> usize {
	match bound {
		std::ops::Bound::Included(v) | std::ops::Bound::Excluded(v) => size(v),
		std::ops::Bound::Unbounded => 0,
	}
}

/// The heap bytes a geometry owns beyond its inline size: its coordinate
/// buffers and the containers holding its rings, lines, polygons and members,
/// so a geometry of many empty parts is not taken for a narrow one.
fn geometry_heap_size(geometry: &crate::val::Geometry) -> usize {
	use std::mem::size_of;

	use crate::val::Geometry;
	fn line(line: &geo::LineString<f64>) -> usize {
		size_of::<geo::Coord<f64>>() * line.0.len()
	}
	fn polygon(polygon: &geo::Polygon<f64>) -> usize {
		line(polygon.exterior())
			+ polygon
				.interiors()
				.iter()
				.map(|ring| size_of::<geo::LineString<f64>>() + line(ring))
				.sum::<usize>()
	}
	match geometry {
		Geometry::Point(_) => 0,
		Geometry::Line(l) => line(l),
		Geometry::Polygon(p) => polygon(p),
		Geometry::MultiPoint(m) => size_of::<geo::Point<f64>>() * m.0.len(),
		Geometry::MultiLine(m) => {
			m.0.iter().map(|l| size_of::<geo::LineString<f64>>() + line(l)).sum()
		}
		Geometry::MultiPolygon(m) => {
			m.0.iter().map(|p| size_of::<geo::Polygon<f64>>() + polygon(p)).sum()
		}
		Geometry::Collection(c) => {
			c.iter().map(|g| size_of::<Geometry>() + geometry_heap_size(g)).sum()
		}
	}
}

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
///
/// SECURITY: when `fetch_full` is `true` the materialised record must go
/// through the *same* field-level processing as an ordinary table scan
/// ([`super::pipeline::filter_and_process_batch`]) and [`fetch_record`]
/// ([`crate::exec::operators::fetch::process_fetched_record`]): read-time
/// `COMPUTED` fields are always evaluated, and field-level SELECT
/// permissions are applied whenever `check_perms` is set. Resolving only the
/// table-level permission here (the previous behaviour) let a graph or
/// reference traversal that yields full edge/record objects expose
/// field-restricted data to callers whose field permissions should hide it,
/// and dropped computed fields — diverging from both the legacy compute
/// engine and a direct `SELECT *` on the same table.
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
		surrealdb_strand::TableName,
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
			let catalog_perm = table_select_permission(table_def.as_deref());
			let perm =
				crate::exec::permission::convert_permission_to_physical_runtime(catalog_perm, ctx)
					.await
					.context("Failed to convert permission")?;
			perm_cache.insert(rid.table.clone(), perm);
		}
	}

	let records = txn
		.get_records(ns_id, db_id, rids, version, cache_policy)
		.await
		.context("Failed to fetch records")?;
	ensure_below_memory_threshold()?;

	// Per-table field state (computed fields + field-level SELECT
	// permissions), built lazily and reused across the records of this batch.
	// `build_field_state` is itself cached per `(table, check_perms)` on the
	// database context, so the cross-batch cost is just a lookup + clone; the
	// local map avoids even that for the common single-table batch.
	let mut field_state_cache: std::collections::HashMap<TableName, FieldState> =
		std::collections::HashMap::new();
	// Computed-field record dereferences must reuse the ambient
	// `skip_fetch_perms` so that, when this runs inside a permission predicate,
	// they don't recurse back into permission checks on cyclic links — matching
	// `fetch_record` (false) vs `fetch_record_no_perms` (true). `check_perms`
	// is always false in the `skip_fetch_perms` case (see `should_check_perms`).
	let skip_fetch_perms = ctx.root().skip_fetch_perms;

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
			let mut value = match Arc::try_unwrap(record) {
				Ok(rec) => rec.data,
				Err(arc) => arc.data.clone(),
			};

			// Apply the same field-level processing as ordinary table scans
			// and `fetch_record`: evaluate read-time computed fields, then
			// (when permissions are enforced) cut field-restricted values.
			// Without this, a graph/reference traversal yielding full
			// records would bypass field-level SELECT permissions and omit
			// computed fields. See the SECURITY note on this function.
			if !field_state_cache.contains_key(&rid.table) {
				let fs = build_field_state(ctx, &rid.table, check_perms, None).await?;
				field_state_cache.insert(rid.table.clone(), fs);
			}
			let field_state = &field_state_cache[&rid.table];
			materialise_fields_with_permissions(
				ctx,
				field_state,
				&mut value,
				skip_fetch_perms,
				check_perms,
			)
			.await?;

			values.push(value);
		} else {
			values.push(Value::RecordId(rid.clone()));
		}
	}
	if fetch_full {
		// Computed fields can make the materialised rows far wider than the
		// records fetched above.
		ensure_below_memory_threshold()?;
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
	let fetched = fetch_and_filter(
		ctx,
		txn,
		ns_id,
		db_id,
		rids,
		select_permission,
		check_perms,
		version,
		cache_policy,
		false,
	)
	.await?;
	Ok(fetched.values)
}

/// As [`fetch_and_filter_records_batch`], also measuring every record the
/// fetch decoded, for a scan that sizes its batches; see [`next_batch_len`].
#[allow(clippy::too_many_arguments)]
pub(crate) async fn fetch_and_filter_records_measured(
	ctx: &ExecutionContext,
	txn: &Transaction,
	ns_id: NamespaceId,
	db_id: DatabaseId,
	rids: &[RecordId],
	select_permission: &crate::exec::permission::PhysicalPermission,
	check_perms: bool,
	version: Option<u64>,
	cache_policy: CachePolicy,
) -> Result<FetchedBatch, ControlFlow> {
	fetch_and_filter(
		ctx,
		txn,
		ns_id,
		db_id,
		rids,
		select_permission,
		check_perms,
		version,
		cache_policy,
		true,
	)
	.await
}

/// The shared body of the fetch helpers above; `measure` fills
/// [`FetchedBatch::fetched_bytes`], which is 0 otherwise.
#[allow(clippy::too_many_arguments)]
async fn fetch_and_filter(
	ctx: &ExecutionContext,
	txn: &Transaction,
	ns_id: NamespaceId,
	db_id: DatabaseId,
	rids: &[RecordId],
	select_permission: &crate::exec::permission::PhysicalPermission,
	check_perms: bool,
	version: Option<u64>,
	cache_policy: CachePolicy,
	measure: bool,
) -> Result<FetchedBatch, ControlFlow> {
	let records = txn
		.get_records(ns_id, db_id, rids, version, cache_policy)
		.await
		.context("Failed to fetch records")?;
	ensure_below_memory_threshold()?;

	let mut values = Vec::with_capacity(rids.len());
	let mut fetched_bytes = 0;
	let mut fetched_rows = 0;
	let mut cloned = false;
	for record in records {
		if record.data.is_none() {
			continue;
		}
		fetched_rows += 1;
		if measure {
			fetched_bytes += approx_value_size(&record.data);
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
			Err(arc) => {
				cloned = true;
				arc.data.clone()
			}
		};
		values.push(value);
	}
	if cloned {
		// A record the transaction's cache still shares is cloned above, after
		// the check that followed the fetch.
		ensure_below_memory_threshold()?;
	}
	Ok(FetchedBatch {
		values,
		fetched_bytes,
		fetched_rows,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	const MAX_ROWS: usize = 1000;

	fn bytes(rows: &[Value]) -> usize {
		rows.iter().map(approx_value_size).sum()
	}

	#[test]
	fn a_record_id_counts_its_key() {
		let key = RecordIdKey::Array(vec![Value::from("x".repeat(4096))].into());
		let rid = Value::RecordId(RecordId {
			table: "t".into(),
			key,
		});
		assert!(approx_value_size(&rid) > 4096, "a wide compound key is sized by its contents");
	}

	fn row_with_floats(n: usize) -> Value {
		Value::from(vec![Value::from(0.5f64); n])
	}

	#[test]
	fn an_empty_previous_batch_keeps_the_row_cap() {
		assert_eq!(next_batch_len(0, 0, MAX_ROWS, 1 << 20, MAX_ROWS), MAX_ROWS);
	}

	#[test]
	fn narrow_rows_keep_the_row_cap() {
		let rows = vec![Value::from("narrow"); 10];
		assert_eq!(next_batch_len(bytes(&rows), rows.len(), 600, 8 << 20, MAX_ROWS), MAX_ROWS);
	}

	#[test]
	fn the_probe_follows_the_budget() {
		assert_eq!(probe_batch_len(2 << 20, 32), 32);
		assert_eq!(probe_batch_len(256 << 10, 32), 4);
		assert_eq!(probe_batch_len(8 << 20, 32), 32);
		assert_eq!(probe_batch_len(1, 32), 1);
	}

	#[test]
	fn a_batch_at_most_doubles() {
		let rows = vec![Value::from("narrow"); 10];
		assert_eq!(next_batch_len(bytes(&rows), rows.len(), 32, 8 << 20, MAX_ROWS), 64);
	}

	#[test]
	fn wide_rows_shrink_the_batch_to_the_budget() {
		let rows = vec![row_with_floats(1024); 4];
		let per_row = approx_value_size(&rows[0]);
		assert_eq!(per_row, std::mem::size_of::<Value>() * 1025);
		assert_eq!(
			next_batch_len(bytes(&rows), rows.len(), MAX_ROWS, 8 << 20, MAX_ROWS),
			(8 << 20) / per_row
		);
	}

	#[test]
	fn a_row_wider_than_the_budget_still_takes_one_row() {
		let rows = vec![row_with_floats(1024)];
		assert_eq!(next_batch_len(bytes(&rows), rows.len(), 32, 1, MAX_ROWS), 1);
	}

	#[test]
	fn an_empty_batch_keeps_the_current_length() {
		assert_eq!(next_batch_len(0, 0, 31, 2 << 20, MAX_ROWS), 31);
	}

	#[test]
	fn a_geometry_counts_its_containers() {
		let empty = || geo::Polygon::new(geo::LineString::<f64>::new(vec![]), vec![]);
		let members: Vec<crate::val::Geometry> =
			(0..5000).map(|_| crate::val::Geometry::Polygon(empty())).collect();
		let collection = Value::Geometry(crate::val::Geometry::Collection(members));
		assert!(
			approx_value_size(&collection) >= 5000 * std::mem::size_of::<crate::val::Geometry>(),
			"a collection of 5000 empty polygons is sized by its members"
		);
		let rings: Vec<geo::LineString<f64>> =
			(0..5000).map(|_| geo::LineString::new(vec![])).collect();
		let polygon = Value::Geometry(crate::val::Geometry::Polygon(geo::Polygon::new(
			geo::LineString::new(vec![]),
			rings,
		)));
		assert!(
			approx_value_size(&polygon) >= 5000 * std::mem::size_of::<geo::LineString<f64>>(),
			"a polygon of 5000 empty interior rings is sized by its rings"
		);
	}

	#[test]
	fn a_geometry_counts_its_coordinates() {
		let ring: Vec<(f64, f64)> = (0..5000).map(|i| (i as f64, 0.0)).collect();
		let polygon =
			Value::Geometry(crate::val::Geometry::Polygon(geo::Polygon::new(ring.into(), vec![])));
		assert!(
			approx_value_size(&polygon) >= 5000 * std::mem::size_of::<geo::Coord<f64>>(),
			"a 5000-point polygon is sized by its coordinates"
		);
	}
}
