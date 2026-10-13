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
		// The loop-invariant scan inputs travel as one shared Arc: the
		// concurrent fan-out captures them per spawned target, and an Arc
		// clone there is a refcount bump instead of four owned clones.
		let scan_args = Arc::new(RefScanArgs {
			table: self.referencing_table.clone(),
			field: self.referencing_field.clone(),
			range_start: self.range_start.clone(),
			range_end: self.range_end.clone(),
		});
		let output_mode = self.output_mode;
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
				&& matches!(scan_args.range_start, Bound::Unbounded)
				&& matches!(scan_args.range_end, Bound::Unbounded);

			// Raw-byte pre-screen for the cache fast path: a table
			// restriction is every matching entry's leading storekey
			// segment, extended by the field segment when both are set. A
			// field restriction without a table is not a prefix (the field
			// sorts after the table inside each entry), so it keeps the
			// decoded comparison alone. The decoded comparisons below stay
			// authoritative either way; the prefix only skips decoding
			// entries the restriction is certain to reject.
			let cache_filter_prefix: Option<Vec<u8>> = match (&scan_args.table, cache_eligible) {
				(Some(table), true) => {
					let mut prefix =
						storekey::encode_vec(table).map_err(anyhow::Error::from_boxed)?;
					if let Some(field) = &scan_args.field {
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

				// Scan the cache-missed targets' reference keys concurrently —
				// bounded, in input order — so a multi-source fan-out overlaps
				// its per-target range scans instead of paying one sequential
				// open-and-drain per target. Each task buffers at most
				// `scan_batch_size` rids, and a target with more references
				// than that finishes sequentially at the ordered merge below,
				// so a supernode never fans out. The batch is processed in
				// contiguous windows of the fan-out width, each scanned and
				// then drained/emitted before the next begins: the aggregate
				// buffered footprint is bounded at one window of capped
				// buffers (`REFERENCE_FANOUT_CONCURRENCY × scan_batch_size`
				// rids) however large the input batch, results stream between
				// windows, and a downstream operator that stops early (LIMIT)
				// drops the stream after one window's work.
				let mut scanned: Vec<Option<TargetRefs>> = Vec::new();
				scanned.resize_with(target_rids.len(), || None);
				// Whether spawning pays for this batch: undecided until the
				// first window has run sequentially as a timing probe, then
				// fixed for the batch's remaining windows.
				#[cfg(not(target_family = "wasm"))]
				let mut fanout_engaged: Option<bool> = None;
				let mut win_start = 0usize;
				while win_start < target_rids.len() {
					let win_end = (win_start + REFERENCE_FANOUT_CONCURRENCY).min(target_rids.len());
					// Spawned rather than merely polled concurrently: on an
					// embedded backend the per-target scans are CPU-bound,
					// and only separate runtime tasks let them use separate
					// cores. wasm has no worker threads and scans every
					// target inline in the drain loop below.
					#[cfg(not(target_family = "wasm"))]
					if fanout_engaged == Some(true) {
						let missed: Vec<usize> = (win_start..win_end)
							.filter(|tgt_idx| !cache_map.contains_key(tgt_idx))
							.collect();
						// A window with one cache-missed target has nothing
						// to overlap; the futures are built only for windows
						// that spawn, so a declined window allocates no
						// captures.
						if missed.len() > 1 {
							let mut pending: std::collections::VecDeque<_> = missed
								.into_iter()
								.map(|tgt_idx| {
									let ctx = ctx.clone();
									let txn = Arc::clone(&txn);
									let rid = target_rids[tgt_idx].clone();
									let scan_args = Arc::clone(&scan_args);
									async move {
										let refs = async {
											let range = compute_ref_key_range(
												ns_id,
												db_id,
												&rid,
												scan_args.table.as_ref(),
												scan_args.field.as_deref(),
												&scan_args.range_start,
												&scan_args.range_end,
												&ctx,
											)
											.await?;
											scan_target_refs(&txn, range, version, scan_batch_size)
												.await
										}
										.await;
										(tgt_idx, refs)
									}
								})
								.collect();
							// A `JoinSet` aborts whatever is still in flight
							// when it drops, so an error or a cancelled
							// stream never leaks detached tasks. On failure
							// the lowest-index target's error surfaces: no
							// further tasks are spawned and the in-flight set
							// is drained, so which error a caller sees never
							// depends on the scheduler. A panicked task
							// surfaces only when no target error was
							// collected — its position carries no meaning.
							let mut set = tokio::task::JoinSet::new();
							let mut first_err: Option<(usize, ControlFlow)> = None;
							let mut join_err: Option<ControlFlow> = None;
							loop {
								if first_err.is_none() && join_err.is_none() {
									while set.len() < REFERENCE_FANOUT_CONCURRENCY {
										let Some(task) = pending.pop_front() else {
											break;
										};
										set.spawn(task);
									}
								}
								let Some(joined) = set.join_next().await else {
									break;
								};
								match joined {
									Ok((tgt_idx, Ok(refs))) => scanned[tgt_idx] = Some(refs),
									Ok((tgt_idx, Err(err))) => {
										if first_err
											.as_ref()
											.is_none_or(|(lowest, _)| tgt_idx < *lowest)
										{
											first_err = Some((tgt_idx, err));
										}
									}
									Err(e) => {
										join_err.get_or_insert_with(|| {
											ControlFlow::Err(anyhow::anyhow!(
												"reference fan-out task failed: {e}"
											))
										});
									}
								}
							}
							if let Some((_, err)) = first_err {
								return Err(err);
							}
							if let Some(err) = join_err {
								return Err(err);
							}
						}
					}
					#[cfg(not(target_family = "wasm"))]
					let win_timer = fanout_engaged.is_none().then(web_time::Instant::now);

					// Emit this window's targets, in input order.
					for tgt_idx in win_start..win_end {
						let rid = &target_rids[tgt_idx];
						// Cache fast path: filter the complete cached set by the
						// requested table/field — entries are held in reference-key
						// byte order, so the surviving subsequence is exactly what
						// the narrowed key scan would have produced. The prefix
						// pre-screen rejects on the raw bytes, so only entries the
						// restriction can accept are ever decoded.
						if let Some(entries) = cache_map.get(&tgt_idx) {
							for entry in entries {
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
								if scan_args.table.as_ref().is_some_and(|t| *t != ft) {
									continue;
								}
								if scan_args.field.as_deref().is_some_and(|f| f != ff) {
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
						let refs = match scanned[tgt_idx].take() {
							Some(refs) => refs,
							// Not prescanned — wasm, a lone miss in its window,
							// or a batch the timing probe kept sequential — so
							// scan here, exactly as every target used to.
							None => {
								let range = compute_ref_key_range(
									ns_id,
									db_id,
									rid,
									scan_args.table.as_ref(),
									scan_args.field.as_deref(),
									&scan_args.range_start,
									&scan_args.range_end,
									&ctx,
								)
								.await?;
								scan_target_refs(&txn, range, version, scan_batch_size).await?
							}
						};
						for found in refs.rids {
							rid_batch.push(found);
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
						// A target larger than one task buffer finishes here,
						// sequentially, exactly as every target used to.
						let Some(range) = refs.resume else {
							continue;
						};
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
					#[cfg(not(target_family = "wasm"))]
					if let Some(started) = win_timer {
						// The first window's sequential run doubles as the
						// timing probe: engage the spawned fan-out for the
						// batch's remaining windows only when the measured
						// per-target cost clears the dispatch-overhead bar,
						// so a cheap batch (a couple of references per
						// target) keeps its exact sequential cost.
						let per_target =
							started.elapsed().as_nanos() as u64 / (win_end - win_start) as u64;
						fanout_engaged = Some(per_target >= REFERENCE_SPAWN_PER_TARGET_NANOS);
					}
					win_start = win_end;
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

/// How many cache-missed targets' reference scans run side by side — and
/// the width of the drain windows the scan processes an input batch in,
/// which bounds the aggregate buffered footprint at one window of capped
/// task buffers and keeps the output streaming between windows.
const REFERENCE_FANOUT_CONCURRENCY: usize = 16;

/// The measured per-target scan cost below which the fan-out does not pay:
/// each spawned target costs a context clone, a handful of `Arc` bumps, a
/// `RecordId` clone and a task dispatch, which together sit at roughly this
/// scale — overlapping work cheaper than that loses more to dispatch than
/// it recovers from parallelism.
#[cfg(not(target_family = "wasm"))]
const REFERENCE_SPAWN_PER_TARGET_NANOS: u64 = 5_000;

/// The loop-invariant inputs of every per-target reference scan — what the
/// requested lookup narrows the key range by — shared across targets as
/// one Arc rather than cloned per spawned task.
struct RefScanArgs {
	table: Option<TableName>,
	field: Option<String>,
	range_start: Bound<Arc<dyn PhysicalExpr>>,
	range_end: Bound<Arc<dyn PhysicalExpr>>,
}

/// The bounded prefix of one target's reference set: up to the task
/// buffer's cap of decoded rids, plus the pre-positioned resume range when
/// the set was larger — the ordered merge finishes such a target
/// sequentially, so concurrent tasks never buffer a supernode.
struct TargetRefs {
	rids: Vec<RecordId>,
	resume: Option<TypedRange<()>>,
}

/// Scans one target's reference keys into an owned, bounded buffer — the
/// self-contained unit the concurrent fan-out runs per target.
async fn scan_target_refs(
	txn: &crate::kvs::Transaction,
	range: TypedRange<()>,
	version: Option<u64>,
	cap: usize,
) -> Result<TargetRefs, ControlFlow> {
	use crate::key::Resumable;
	let full = range.clone();
	let mut cursor = txn
		.open_keys_cursor(range, Direction::Forward, 0, version)
		.await
		.context("Failed to open reference cursor")?;
	let mut rids = Vec::new();
	let mut resume: Option<TypedRange<()>> = None;
	loop {
		let mut decode_err: Option<anyhow::Error> = None;
		let stats = cursor
			.for_each(crate::kvs::NORMAL_BATCH_SIZE, &mut |key| match ReferenceKey::decode_key(key)
			{
				Ok(decoded) => {
					rids.push(RecordId {
						table: decoded.foreign_table.into_owned(),
						key: decoded.foreign_key.into_owned(),
					});
					if rids.len() >= cap {
						resume = Some(full.clone().resume_after(key, Direction::Forward));
						Ok(std::ops::ControlFlow::Break(()))
					} else {
						Ok(std::ops::ControlFlow::Continue(()))
					}
				}
				Err(e) => {
					decode_err = Some(e);
					Ok(std::ops::ControlFlow::Break(()))
				}
			})
			.await
			.context("Failed to scan reference")?;
		if let Some(e) = decode_err {
			return Err(e).context("Failed to decode ref key")?;
		}
		if resume.is_some() || stats.rows == 0 {
			break;
		}
	}
	Ok(TargetRefs {
		rids,
		resume,
	})
}

#[cfg(all(feature = "kv-mem", not(target_family = "wasm")))]
#[cfg(test)]
mod window_tests {
	use surrealdb_cnf::ConfigMap;

	use super::*;
	use crate::exec::operators::test_util::{TestDb, ValuesOperator, collect};
	use crate::val::Value;

	fn rid(table: &str, key: impl std::fmt::Display) -> Value {
		Value::RecordId(RecordId::new(table.into(), key.to_string()))
	}

	/// Results must be complete and exact across every boundary the
	/// windowed fan-out introduces: window edges, the per-task buffer cap
	/// (an over-cap target finishes through its sequential resume tail),
	/// and the flush size — no row dropped, none double-emitted.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn results_are_exact_across_window_and_cap_boundaries() {
		const TARGETS: usize = 40;
		const HUB_REFS: usize = 10;
		let mut setup = String::from(
			"DEFINE TABLE person SCHEMALESS; DEFINE TABLE post SCHEMALESS; \
			 DEFINE FIELD author ON post TYPE record<person> REFERENCE;",
		);
		setup.push_str("CREATE person:p0;");
		for j in 0..HUB_REFS {
			setup.push_str(&format!("CREATE post:h{j} SET author = person:p0;"));
		}
		for i in 1..TARGETS {
			setup.push_str(&format!(
				"CREATE person:p{i}; CREATE post:q{i} SET author = person:p{i};"
			));
		}
		// A scan_batch_size of 4 is both the per-task buffer cap (under the
		// hub's ten references) and the flush size, so every boundary is
		// crossed.
		let db = TestDb::new_with_config(
			&setup,
			ConfigMap::empty().with_key_value("scan_batch_size", "4"),
		)
		.await;
		let ctx = db.exec_ctx().await;

		let scan = ReferenceScan::new(
			ValuesOperator::new((0..TARGETS).map(|i| rid("person", format!("p{i}"))).collect()),
			Some("post".into()),
			Some("author".to_string()),
			ReferenceScanOutput::RecordId,
			Bound::Unbounded,
			Bound::Unbounded,
			None,
		);
		let op: Arc<dyn ExecOperator> = Arc::new(scan);
		let mut out = collect(&op, &ctx).await;
		out.sort();

		let mut expected: Vec<Value> = (0..HUB_REFS)
			.map(|j| rid("post", format!("h{j}")))
			.chain((1..TARGETS).map(|i| rid("post", format!("q{i}"))))
			.collect();
		expected.sort();
		assert_eq!(out, expected);
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
