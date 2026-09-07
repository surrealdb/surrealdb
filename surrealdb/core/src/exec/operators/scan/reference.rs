//! Reference scanning operator for the streaming execution engine.
//!
//! This operator scans record references (the `<~` operator) to find records
//! that reference a given source record. Unlike graph edges which are explicit
//! relationships, references are field-level links tracked by the database.

use std::borrow::Cow;
use std::ops::Bound;
use std::sync::Arc;

use common::future::stream::{self, Yielder};
use futures::StreamExt;
use surrealdb_datastore::values::inline_cache::{CacheValue, RefCacheEntry};

use super::common::{
	evaluate_bound_key, extract_record_ids_into, resolve_record_batch, resolve_version_stamp,
};
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::exec::permission::{
	PhysicalPermission, should_check_perms, validate_record_user_access,
};
use crate::exec::{
	AccessMode, ContextLevel, ControlFlowExt, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::expr::ControlFlow;
use crate::iam::Action;
use crate::key::schema::{
	DbRoot, RefCacheKey, ReferenceForeignFieldPrefix, ReferenceForeignTablePrefix,
	ReferenceIdPrefix, ReferenceKey,
};
use crate::key::{KVKeyDecode, TypedRange};
use crate::kvs::{CachePolicy, Direction};
use crate::val::{RecordId, RecordIdKey, TableName};

/// What kind of output the ReferenceScan should produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReferenceScanOutput {
	/// Return only the referencing record IDs
	#[default]
	RecordId,
	/// Return full referencing records (fetched from the datastore).
	/// Required when downstream operators need field access (e.g. Sort, Split).
	FullRecord,
}

/// Scans record references for records received from a child operator stream.
///
/// This operator implements a nested-loop-join pattern: it reads RecordIds from
/// its `input` child operator stream, then for each RecordId scans references
/// to find records that reference it. It implements the `<~` (reference lookup)
/// operator.
///
/// Example: For `person:alice<~post`, this finds all `post` records that
/// have a field referencing `person:alice`.
#[derive(Debug, Clone)]
pub struct ReferenceScan {
	/// Child operator that provides target RecordId(s) being referenced
	pub(crate) input: Arc<dyn ExecOperator>,

	/// The table that contains the referencing records (e.g., `post`).
	/// If None, scans ALL tables that reference the target (wildcard `<~?`).
	pub(crate) referencing_table: Option<TableName>,

	/// Optional: The specific field in the referencing table that holds the reference
	/// If None, scans all fields that reference the target
	pub(crate) referencing_field: Option<String>,

	/// What to output: RecordId or FullRecord
	pub(crate) output_mode: ReferenceScanOutput,

	/// Range start bound for the referencing record IDs.
	/// When `Unbounded`, starts from the field/table prefix.
	pub(crate) range_start: Bound<Arc<dyn PhysicalExpr>>,

	/// Range end bound for the referencing record IDs.
	/// When `Unbounded`, ends at the field/table suffix.
	pub(crate) range_end: Bound<Arc<dyn PhysicalExpr>>,

	/// Optional VERSION timestamp for time-travel queries.
	pub(crate) version: Option<Arc<dyn PhysicalExpr>>,

	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl ReferenceScan {
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		referencing_table: Option<TableName>,
		referencing_field: Option<String>,
		output_mode: ReferenceScanOutput,
		range_start: Bound<Arc<dyn PhysicalExpr>>,
		range_end: Bound<Arc<dyn PhysicalExpr>>,
		version: Option<Arc<dyn PhysicalExpr>>,
	) -> Self {
		Self {
			input,
			referencing_table,
			referencing_field,
			output_mode,
			range_start,
			range_end,
			version,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}
impl ExecOperator for ReferenceScan {
	fn name(&self) -> &'static str {
		"ReferenceScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![(
			"table".to_string(),
			self.referencing_table
				.as_ref()
				.map(|t| t.as_str().to_string())
				.unwrap_or_else(|| "?".to_string()),
		)];
		if let Some(field) = &self.referencing_field {
			attrs.push(("field".to_string(), field.clone()));
		}
		if self.output_mode == ReferenceScanOutput::FullRecord {
			attrs.push(("output".to_string(), "full_record".to_string()));
		}
		if !matches!(self.range_start, Bound::Unbounded)
			|| !matches!(self.range_end, Bound::Unbounded)
		{
			attrs.push(("range".to_string(), "bounded".to_string()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		// ReferenceScan needs database context, combined with expression contexts
		self.input.required_context().max(ContextLevel::Database)
	}

	fn access_mode(&self) -> AccessMode {
		let mut mode = self.input.access_mode();
		if let Some(ref version) = self.version {
			mode = mode.combine(version.access_mode());
		}
		mode
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();
		// SECURITY (GHSA-2v9j): confine the reference resolution to the caller's
		// tenant. Like the graph scan, a reference scan can be rooted at a literal
		// record, so it is not guaranteed to sit downstream of a validated entry
		// scan; enforce the namespace/database boundary here.
		validate_record_user_access(&db_ctx)?;
		// SECURITY: reference scan results bypass `Document::pluck_select`, so
		// the referencing table's SELECT permission must be enforced here
		// (same family as the graph scan check). `resolve_record_batch`
		// enforces the table-level permission and — in `FullRecord` mode —
		// additionally applies field-level SELECT permissions and computed
		// fields, matching a direct `SELECT *` on the referencing table.
		let check_perms = should_check_perms(&db_ctx, Action::View)?;
		let mut input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		let referencing_table = self.referencing_table.clone();
		let referencing_field = self.referencing_field.clone();
		let output_mode = self.output_mode;
		let range_start = self.range_start.clone();
		let range_end = self.range_end.clone();
		let scan_batch_size = ctx.root().ctx.config.exec.scan_batch_size;
		let ctx = ctx.clone();
		let fetch_full = output_mode == ReferenceScanOutput::FullRecord;
		let version_expr = self.version.clone();
		let metrics = Arc::clone(&self.metrics);
		let record_metrics = metrics.is_enabled();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			let txn = ctx.txn();
			let ns_id = db_ctx.ns_ctx.ns.namespace_id;
			let db_id = db_ctx.db.database_id;
			let mut perm_cache: std::collections::HashMap<
				surrealdb_strand::TableName,
				PhysicalPermission,
			> = std::collections::HashMap::new();

			// Resolve VERSION timestamp; see [`resolve_version_stamp`] for
			// why we prefer the stamp already set by the enclosing
			// `VersionScope` over re-evaluating `version_expr` here.
			let version: Option<u64> = resolve_version_stamp(&ctx, version_expr.as_ref()).await?;

			// Inline reference caches hold a record's complete current
			// reference set, so they are usable only when this scan reads
			// current data with no key-range bounds; a table or field
			// restriction is a filter over the complete set and stays
			// cache-servable.
			let cache_eligible = version.is_none()
				&& matches!(range_start, Bound::Unbounded)
				&& matches!(range_end, Bound::Unbounded);

			// Raw-byte pre-screen for the cache fast path: a table
			// restriction is every matching entry's leading storekey
			// segment, extended by the field segment when both are set. A
			// field restriction without a table is not a prefix (the field
			// sorts after the table inside each entry), so it keeps the
			// decoded comparison alone. The decoded comparisons below stay
			// authoritative either way; the prefix only skips decoding
			// entries the restriction is certain to reject.
			let cache_filter_prefix: Option<Vec<u8>> = match (&referencing_table, cache_eligible) {
				(Some(table), true) => {
					let mut prefix =
						storekey::encode_vec(table).map_err(anyhow::Error::from_boxed)?;
					if let Some(field) = &referencing_field {
						prefix.extend(
							storekey::encode_vec(field.as_str())
								.map_err(anyhow::Error::from_boxed)?,
						);
					}
					Some(prefix)
				}
				_ => None,
			};

			// Read from the child operator stream and extract RecordIds
			let mut rid_batch: Vec<RecordId> = Vec::with_capacity(scan_batch_size);

			while let Some(batch_result) = input_stream.next().await {
				let batch = batch_result?;
				let target_rids: Vec<RecordId> = batch
					.into_iter()
					.flat_map(|v| {
						let mut rids = Vec::new();
						extract_record_ids_into(v, &mut rids);
						rids
					})
					.collect();

				// One multi-get fetches every eligible target's cache key for
				// this input batch — the fan-out the caches exist to amortise.
				// A live hit is that record's complete reference set, held in
				// its raw entry form until the per-target loop below consumes
				// it, so at most one target's set is ever decoded at a time;
				// anything else falls back to the key scan.
				let mut cache_map: std::collections::HashMap<usize, Vec<RefCacheEntry>> =
					std::collections::HashMap::new();
				if cache_eligible {
					// One catalog lookup per distinct target table in this
					// batch — targets overwhelmingly share one table.
					let mut table_capped: std::collections::HashMap<TableName, bool> =
						std::collections::HashMap::new();
					let mut slots: Vec<usize> = Vec::new();
					let mut keys: Vec<RefCacheKey> = Vec::new();
					for (tgt_idx, rid) in target_rids.iter().enumerate() {
						let capped = match table_capped.get(&rid.table) {
							Some(capped) => *capped,
							None => {
								let tb = txn
									.get_tb(ns_id, db_id, &rid.table, None)
									.await
									.context("Failed to resolve the referenced record's table")?;
								let capped =
									crate::idx::inline_cache::effective_refs_cap(tb.as_deref())
										.is_some();
								table_capped.insert(rid.table.clone(), capped);
								capped
							}
						};
						if !capped {
							continue;
						}
						slots.push(tgt_idx);
						keys.push(RefCacheKey {
							ns: ns_id,
							db: db_id,
							tb: Cow::Borrowed(&rid.table),
							id: Cow::Borrowed(&rid.key),
						});
					}
					if !keys.is_empty() {
						let values = txn
							.get_many_key(keys, None)
							.await
							.context("Failed to read the inline reference caches")?;
						let mut hits: u64 = 0;
						let mut misses: u64 = 0;
						for (tgt_idx, value) in slots.into_iter().zip(values) {
							match value {
								Some(CacheValue::Live(entries)) => {
									hits += 1;
									cache_map.insert(tgt_idx, entries);
								}
								_ => misses += 1,
							}
						}
						if record_metrics {
							metrics.add_cache_hits(hits);
							metrics.add_cache_misses(misses);
						}
					}
				}

				// Scan references for each target record
				for (tgt_idx, rid) in target_rids.iter().enumerate() {
					// Cache fast path: filter the complete cached set by the
					// requested table/field — entries are held in reference-key
					// byte order, so the surviving subsequence is exactly what
					// the narrowed key scan would have produced. The prefix
					// pre-screen rejects on the raw bytes, so only entries the
					// restriction can accept are ever decoded.
					if let Some(entries) = cache_map.remove(&tgt_idx) {
						for entry in &entries {
							if let Some(prefix) = &cache_filter_prefix
								&& !entry.reference.starts_with(prefix)
							{
								continue;
							}
							let (ft, ff, fk) =
								storekey::decode_borrow::<(TableName, String, RecordIdKey)>(
									&entry.reference,
								)
								.map_err(|e| {
									anyhow::anyhow!("Failed to decode a cached reference: {e}")
								})?;
							if referencing_table.as_ref().is_some_and(|t| *t != ft) {
								continue;
							}
							if referencing_field.as_deref().is_some_and(|f| f != ff) {
								continue;
							}
							rid_batch.push(RecordId {
								table: ft,
								key: fk,
							});
							if rid_batch.len() >= scan_batch_size {
								let values = resolve_record_batch(
									&ctx,
									&txn,
									ns_id,
									db_id,
									&rid_batch,
									fetch_full,
									check_perms,
									version,
									CachePolicy::ReadWrite,
									&mut perm_cache,
								)
								.await?;
								yielder.emit(ValueBatch::new(values)).await;
								rid_batch.clear();
							}
						}
						continue;
					}
					let range = compute_ref_key_range(
						ns_id,
						db_id,
						rid,
						referencing_table.as_ref(),
						referencing_field.as_deref(),
						&range_start,
						&range_end,
						&ctx,
					)
					.await?;

					let mut cursor = txn
						.open_keys_cursor(range, Direction::Forward, 0, version)
						.await
						.context("Failed to open reference cursor")?;
					loop {
						// Decode each ref key straight from the cursor's borrowed
						// bytes into an owned `RecordId` — no per-key buffer copy.
						let mut decode_err: Option<anyhow::Error> = None;
						let stats = cursor
							.for_each(crate::kvs::NORMAL_BATCH_SIZE, &mut |key| {
								match ReferenceKey::decode_key(key) {
									Ok(decoded) => {
										rid_batch.push(RecordId {
											table: decoded.foreign_table.into_owned(),
											key: decoded.foreign_key.into_owned(),
										});
										Ok(std::ops::ControlFlow::Continue(()))
									}
									Err(e) => {
										decode_err = Some(e);
										Ok(std::ops::ControlFlow::Break(()))
									}
								}
							})
							.await
							.context("Failed to scan reference")?;
						if let Some(e) = decode_err {
							return Err(e).context("Failed to decode ref key")?;
						}
						if stats.rows == 0 {
							break;
						}
						// Resolve full batches before fetching the next chunk
						// from the cursor — keeps the cursor's reusable buffer
						// free for the next call and bounds memory.
						if rid_batch.len() >= scan_batch_size {
							let values = resolve_record_batch(
								&ctx,
								&txn,
								ns_id,
								db_id,
								&rid_batch,
								fetch_full,
								check_perms,
								version,
								CachePolicy::ReadWrite,
								&mut perm_cache,
							)
							.await?;
							yielder.emit(ValueBatch::new(values)).await;
							rid_batch.clear();
						}
					}
				}
			}

			// Yield remaining batch
			if !rid_batch.is_empty() {
				let values = resolve_record_batch(
					&ctx,
					&txn,
					ns_id,
					db_id,
					&rid_batch,
					fetch_full,
					check_perms,
					version,
					CachePolicy::ReadWrite,
					&mut perm_cache,
				)
				.await?;
				yielder.emit(ValueBatch::new(values)).await;
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "ReferenceScan", &self.metrics))
	}
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Compute the KV key range for a reference scan.
///
/// Dispatches to the bound matching the combination of table, field, and range
/// bounds that was supplied: each of them narrows the scan by one more field of
/// the reference key.
#[allow(clippy::too_many_arguments)]
async fn compute_ref_key_range(
	ns_id: NamespaceId,
	db_id: DatabaseId,
	rid: &RecordId,
	referencing_table: Option<&TableName>,
	referencing_field: Option<&str>,
	range_start: &Bound<Arc<dyn PhysicalExpr>>,
	range_end: &Bound<Arc<dyn PhysicalExpr>>,
	ctx: &ExecutionContext,
) -> Result<TypedRange<()>, ControlFlow> {
	let has_range =
		!matches!(range_start, Bound::Unbounded) || !matches!(range_end, Bound::Unbounded);

	let prefix = DbRoot {
		ns: ns_id,
		db: db_id,
	};

	if has_range {
		// Range-bounded scan requires both table and field
		let table = referencing_table.ok_or_else(|| {
			anyhow::anyhow!("Range-bounded reference scans require a referencing table")
		})?;
		let field = referencing_field.ok_or_else(|| {
			anyhow::anyhow!(
				"Cannot scan a specific range of record references without a referencing field"
			)
		})?;

		// The bounds are on the referencing record's key, which is the field the
		// reference key carries after the referring field.
		let start = match range_start {
			Bound::Included(x) => Bound::Included(Cow::Owned(evaluate_bound_key(x, ctx).await?)),
			Bound::Excluded(x) => Bound::Excluded(Cow::Owned(evaluate_bound_key(x, ctx).await?)),
			Bound::Unbounded => Bound::Unbounded,
		};

		let end = match range_end {
			Bound::Included(x) => Bound::Included(Cow::Owned(evaluate_bound_key(x, ctx).await?)),
			Bound::Excluded(x) => Bound::Excluded(Cow::Owned(evaluate_bound_key(x, ctx).await?)),
			Bound::Unbounded => Bound::Unbounded,
		};

		Ok(ReferenceForeignFieldPrefix {
			ns: prefix.ns,
			db: prefix.db,
			tb: Cow::Borrowed(&rid.table),
			id: Cow::Borrowed(&rid.key),
			foreign_table: Cow::Borrowed(table),
			foreign_field: Cow::Borrowed(field),
		}
		.range_where((start, end))?)
	} else if let Some(table) = referencing_table {
		if let Some(field) = referencing_field {
			Ok(ReferenceForeignFieldPrefix {
				ns: prefix.ns,
				db: prefix.db,
				tb: Cow::Borrowed(&rid.table),
				id: Cow::Borrowed(&rid.key),
				foreign_table: Cow::Borrowed(table),
				foreign_field: Cow::Borrowed(field),
			}
			.range()?)
		} else {
			Ok(ReferenceForeignTablePrefix {
				ns: prefix.ns,
				db: prefix.db,
				tb: Cow::Borrowed(&rid.table),
				id: Cow::Borrowed(&rid.key),
				foreign_table: Cow::Borrowed(table),
			}
			.range()?)
		}
	} else {
		Ok(ReferenceIdPrefix {
			ns: prefix.ns,
			db: prefix.db,
			tb: Cow::Borrowed(&rid.table),
			id: Cow::Borrowed(&rid.key),
		}
		.range()?)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::CurrentValueSource;

	#[test]
	fn test_reference_scan_attrs() {
		let scan = ReferenceScan::new(
			Arc::new(CurrentValueSource::new()),
			Some("post".into()),
			Some("author".to_string()),
			ReferenceScanOutput::RecordId,
			Bound::Unbounded,
			Bound::Unbounded,
			None,
		);

		assert_eq!(scan.name(), "ReferenceScan");
		let attrs = scan.attrs();
		assert!(attrs.iter().any(|(k, v)| k == "table" && v == "post"));
		assert!(attrs.iter().any(|(k, v)| k == "field" && v == "author"));
	}
}
