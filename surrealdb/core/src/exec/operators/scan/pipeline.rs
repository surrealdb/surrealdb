//! Shared scan pipeline infrastructure.
//!
//! Contains the types and functions reused across multiple scan operators
//! (DynamicScan, TableScan, RecordIdScan, etc.):
//!
//! - [`ScanPipeline`] — per-batch filter + computed-fields + limit/start pipeline
//! - [`FieldState`] / [`ComputedFieldDef`] — cached field definitions
//! - [`build_field_state`] — resolves computed fields and field permissions
//! - [`filter_and_process_batch`] — single-pass permission + field processing
//! - [`kv_scan_stream`] / [`decode_record`] — raw KV range scan helpers
//! - [`range_start_key`] / [`range_end_key`] — RecordId range key encoding
//! - [`eval_limit_expr`] — LIMIT/START expression evaluation
//! - [`determine_scan_direction`] — ORDER BY → scan direction

// The IR lowering emits the runtime these items serve, so the dead-code lint is allowed for the
// module.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use common::future::stream::{self, Yielder};
use smallvec::SmallVec;

use crate::catalog::providers::TableProvider;
use crate::catalog::{Generation, Permission, TableLayouts};
use crate::exec::migrate::{Replay, stamp_of};
use crate::exec::packed_view::AdmittedRow;
use crate::exec::permission::{
	Derivation, GuardKind, PhysicalPermission, check_permission_for_value, judge_before_derivation,
	plan_table_derivations, read_permission_to_physical,
};
use crate::exec::pre_decode_filter::{PreDecodeFilter, PreDecodeFilterOutcome};
use crate::exec::topk_pushdown::TopKThresholdProbe;
use crate::exec::{Clause, EvalContext, ExecutionContext, ValueBatch, ValueBatchStream};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::key::schema::RecordKey;
use crate::key::{KVKeyDecode, RawRange};
use crate::kvs::{Admission, Direction, RowGate, Transaction};
use crate::val::{RecordId, TableName, Value};

/// A raw computed field entry before topological sorting:
/// `(field_name, clause, optional_kind, dependency_field_names, auth_limit, reenters)`.
type RawComputedField =
	(String, Clause, Option<crate::expr::Kind>, Vec<String>, Option<crate::iam::AuthLimit>, bool);

// =============================================================================
// ScanPipeline
// =============================================================================

/// Inline pipeline that performs all per-batch operations (filtering, computed
/// fields, permissions, limit/start) in a single pass with minimal await
/// boundaries. Limit/start state is tracked across batches so the logic is
/// written once rather than duplicated in every scan path.
pub(crate) struct ScanPipeline {
	permission: PhysicalPermission,
	predicate: Option<Clause>,
	field_state: FieldState,
	check_perms: bool,
	/// Cached at construction: true when filter_and_process_batch must run.
	needs_processing: bool,
	/// Maximum rows to emit (pushed-down LIMIT).
	limit: Option<usize>,
	/// Rows to skip after filtering (pushed-down START, adjusted for pre_skip).
	start: usize,
	/// How many post-filter rows have been skipped so far.
	skipped: usize,
	/// How many rows have been emitted so far.
	emitted: usize,
	/// See [`Self::materialised_bytes`].
	materialised_bytes: usize,
}

impl ScanPipeline {
	/// Check whether any post-decode processing (permission filtering,
	/// computed fields, field-level permissions, or WHERE predicate) is
	/// needed.
	///
	/// This is cached internally so that [`process_batch`] can skip work
	/// when nothing is needed.
	pub(crate) fn compute_needs_processing(
		permission: &PhysicalPermission,
		field_state: &FieldState,
		check_perms: bool,
		predicate: Option<&Clause>,
	) -> bool {
		!matches!(permission, PhysicalPermission::Allow)
			|| !field_state.computed_fields.is_empty()
			|| (check_perms && !field_state.field_permissions.is_empty())
			|| predicate.is_some()
	}

	/// Check whether any operation that **removes rows** is active.
	///
	/// Row-modifying operations (computed fields, field-level permissions)
	/// preserve row count and positional ordering, so `pre_skip` and
	/// `effective_storage_limit` can safely be pushed to the KV layer
	/// even when they are present. Only table-level permission filtering
	/// and WHERE predicates can change which rows survive, preventing
	/// positional pushdown.
	pub(crate) fn compute_needs_row_filtering(
		permission: &PhysicalPermission,
		predicate: Option<&Clause>,
	) -> bool {
		!matches!(permission, PhysicalPermission::Allow) || predicate.is_some()
	}

	pub(crate) fn new(
		permission: PhysicalPermission,
		predicate: Option<Clause>,
		field_state: FieldState,
		check_perms: bool,
		limit: Option<usize>,
		start: usize,
	) -> Self {
		let needs_processing = Self::compute_needs_processing(
			&permission,
			&field_state,
			check_perms,
			predicate.as_ref(),
		);
		Self {
			permission,
			predicate,
			field_state,
			check_perms,
			needs_processing,
			limit,
			start,
			skipped: 0,
			emitted: 0,
			materialised_bytes: 0,
		}
	}

	/// Returns true when limit or start tracking is active.
	fn has_limit(&self) -> bool {
		self.limit.is_some() || self.start > 0
	}

	/// Process a single batch in-place: filter, compute fields, apply
	/// permissions, then apply limit/start. Returns `false` when the
	/// limit has been reached and the caller should stop iterating.
	///
	/// A batch that materialises computed fields is checked against
	/// `SURREAL_MEMORY_THRESHOLD`; see [`filter_and_process_batch`].
	pub(crate) async fn process_batch(
		&mut self,
		batch: &mut Vec<Value>,
		ctx: &ExecutionContext,
	) -> Result<bool, ControlFlow> {
		// Phase 1: filter + process, per record
		self.materialised_bytes = 0;
		if self.needs_processing {
			self.materialised_bytes = filter_and_process_batch(
				batch,
				&self.permission,
				self.predicate.as_ref(),
				ctx,
				&self.field_state,
				self.check_perms,
			)
			.await?;
		}

		// Phase 2: limit/start tracking
		if self.has_limit() && !batch.is_empty() {
			// Apply start offset
			if self.skipped < self.start {
				let remaining_to_skip = self.start - self.skipped;
				if batch.len() <= remaining_to_skip {
					// Entire batch falls within the start offset -- discard it
					self.skipped += batch.len();
					batch.clear();
					return Ok(true);
				}
				self.skipped = self.start;
				batch.drain(..remaining_to_skip);
			}
			// Apply limit
			if let Some(limit) = self.limit {
				let remaining = limit.saturating_sub(self.emitted);
				if batch.len() > remaining {
					batch.truncate(remaining);
				}
			}
			self.emitted += batch.len();
		}

		// Continue iterating unless the limit has been reached.
		Ok(self.limit.is_none_or(|l| self.emitted < l))
	}
}

// =============================================================================
// Scan direction
// =============================================================================

/// Determine scan direction from ORDER BY clause.
/// Returns Backward if the first ORDER BY is `id DESC`, otherwise Forward.
pub(crate) fn determine_scan_direction(
	order: Option<&crate::expr::logical::LogicalOrdering>,
) -> Direction {
	use crate::exec::planner::util::idiom_is_id;
	use crate::expr::logical::LogicalOrdering;
	if let Some(LogicalOrdering::Order(order_list)) = order
		&& let Some(first) = order_list.first()
		&& !first.direction
		&& idiom_is_id(&first.value)
	{
		Direction::Backward
	} else {
		Direction::Forward
	}
}

// =============================================================================
// KV scan stream
// =============================================================================

/// Produce a `ValueBatchStream` from a raw KV range scan.
///
/// `range` is a record region rather than a typed range: a record's `id` comes
/// from its key, so every value has to be decoded against the key it was stored
/// under (see [`decode_record`]).
///
/// `gate` is the generation check of the table the range reads. Each row is
/// checked against it before the pre-decode filter, the top-k probe or the
/// decode sees any field of the row, so a row of a generation the table does
/// not list is refused rather than filtered out. A row naming an unlisted
/// generation breaks the visit once: the gate reloads the table's layouts, and
/// the row is read again before the scan goes on, in the order it was stored.
/// The gate refreshes its layouts before each cursor batch
/// ([`RowGate::refresh`]), so a batch is admitted and decoded against the
/// layouts the transaction holds when it starts, and a packed row is decoded
/// with the layouts that admitted it.
///
/// When `pre_skip > 0`, that many entries are skipped at the KV storage layer
/// before any data is returned, avoiding I/O, allocation, and deserialization
/// for rows that will be discarded anyway (the fast-path optimisation for
/// `START` without a pushdown predicate).
///
/// When `limit_hint` is provided, the first batch is capped to that count so
/// small-limit queries (e.g. `LIMIT 10`) don't fetch a full batch from
/// storage. Subsequent batches use [`crate::kvs::NORMAL_BATCH_SIZE`].
///
/// Iterates the cursor's borrowed `&[u8]` slices directly — record decode
/// happens inline, no intermediate owned `Vec<u8>` allocation per row. The one
/// row a reload holds is copied.
///
/// `replay` is the table's migration steps, when it has any. The pre-decode
/// filter and the top-k probe judge only a row stamped at or after the table's
/// newest step: a row behind a step is handed on decoded, for the consumer to
/// bring through the steps first ([`Replay::apply`]), and every batch carries
/// each row's stamp ([`ValueBatch::stamped`]). A table with no steps passes
/// `None` and its batches carry no stamps.
#[allow(clippy::too_many_arguments)]
pub(crate) fn kv_scan_stream(
	txn: Arc<Transaction>,
	range: RawRange,
	mut gate: RowGate,
	version: Option<u64>,
	storage_limit: Option<usize>,
	direction: Direction,
	pre_skip: usize,
	limit_hint: Option<u32>,
	pre_decode_filter: Option<Arc<PreDecodeFilter>>,
	topk_probe: Option<Arc<TopKThresholdProbe>>,
	replay: Option<Replay>,
	scalars: ScalarRows,
) -> ValueBatchStream {
	let skip = pre_skip.min(u32::MAX as usize) as u32;
	let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
		let mut cursor = txn
			.open_vals_cursor_raw(range, direction, skip, version)
			.await
			.context("Failed to open scan cursor")?;
		let mut first = true;
		let mut yielded: usize = 0;
		loop {
			// Each fetch is capped by NORMAL_BATCH_SIZE, by the remaining
			// `storage_limit` (so a `LIMIT 600` query never asks storage for
			// more than 600 records total), and on the first iteration by
			// `limit_hint` (small-LIMIT fast path; subsequent batches may
			// need to over-fetch when row filtering reduces the visible
			// count downstream, so the hint applies only once).
			let mut batch_size = crate::kvs::NORMAL_BATCH_SIZE;
			if first && let Some(h) = limit_hint {
				batch_size = batch_size.min(h);
			}
			if let Some(cap) = storage_limit {
				let remaining = cap.saturating_sub(yielded);
				let remaining_u32 = remaining.min(u32::MAX as usize) as u32;
				batch_size = batch_size.min(remaining_u32);
			}
			if batch_size == 0 {
				break;
			}
			// No more records than the fuel left: the batch is charged once it
			// returns.
			let batch_size = crate::exec::vm::fuel::batch(batch_size);
			// Drive the cursor one borrowed row at a time. The visitor admits
			// each row's generation, runs the pre-decode filter and decodes
			// survivors straight from the engine's borrowed bytes;
			// `decode_record` yields a fully owned `Value`, so the raw bytes
			// are never retained and the engine hands them over with no
			// per-row copy.
			let mut decoded: Vec<Value> = Vec::with_capacity(batch_size as usize);
			let mut stamps: Vec<Option<Generation>> = Vec::new();
			// A row's failure is a `ControlFlow`, which can't travel through
			// the visitor's storage-error channel — stash it and re-raise it
			// below, outside the cursor borrow.
			let mut decode_err: Option<ControlFlow> = None;
			// The row that named a generation the gate does not list yet. The
			// cursor has consumed it, and it is the last row the visit read.
			let mut held: Option<(Vec<u8>, Vec<u8>)> = None;
			let pdf = pre_decode_filter.as_ref();
			// Snapshot the TopK rejection threshold once per cursor batch.
			// The sort publishes monotonically-tightening values, so a stale
			// snapshot only under-rejects (bounded by one batch) — sound, and
			// it keeps the lock off the per-row path. `None` until the
			// downstream heap fills (or when no publisher was installed).
			let topk_threshold = topk_probe.as_ref().and_then(|p| p.snapshot());
			let mut topk_skipped: u64 = 0;
			// The batch is admitted and decoded against one view of the
			// table's layouts: the visitor is synchronous and runs no
			// statement, so nothing moves the transaction's layout epoch
			// between this refresh and the end of the batch.
			gate.refresh(&txn).await?;
			let layouts: &TableLayouts = gate.layouts();
			let stats = cursor
				.for_each(batch_size, &mut |key, val| {
					match gate.admit(val) {
						Admission::Admit => {}
						Admission::Reload => {
							held = Some((key.to_vec(), val.to_vec()));
							return Ok(std::ops::ControlFlow::Break(()));
						}
						Admission::Refuse(e) => {
							decode_err = Some(ControlFlow::Err(e));
							return Ok(std::ops::ControlFlow::Break(()));
						}
					}
					let row = ScanRow {
						pdf,
						probe: topk_probe.as_ref(),
						threshold: topk_threshold.as_deref(),
						layouts: Some(layouts),
						replay: replay.as_ref(),
						scalars,
					};
					match row.read(key, val, &mut decoded, &mut stamps, &mut topk_skipped) {
						Ok(()) => Ok(std::ops::ControlFlow::Continue(())),
						Err(cf) => {
							decode_err = Some(cf);
							Ok(std::ops::ControlFlow::Break(()))
						}
					}
				})
				.await
				.context("Failed to scan record")?;
			// Re-raise a stashed failure as the stream's terminal error, once the cursor borrow has
			// ended. A `?` on an `Err` already ends the stream, so no explicit `return` is
			// needed.
			if let Some(cf) = decode_err {
				Err(cf)?;
			}
			if let Some((key, val)) = held {
				gate.reload(&txn).await?;
				match gate.admit(&val) {
					Admission::Admit => {}
					Admission::Refuse(e) => Err(ControlFlow::Err(e))?,
					Admission::Reload => Err(ControlFlow::Err(anyhow::anyhow!(
						"a row's generation was unlisted again after the layouts reloaded"
					)))?,
				}
				ScanRow {
					pdf,
					probe: topk_probe.as_ref(),
					threshold: topk_threshold.as_deref(),
					layouts: Some(&**gate.layouts()),
					replay: replay.as_ref(),
					scalars,
				}
				.read(&key, &val, &mut decoded, &mut stamps, &mut topk_skipped)?;
			}
			if topk_skipped > 0
				&& let Some(m) = topk_probe.as_ref().and_then(|p| p.metrics())
			{
				m.add_skipped_rows(topk_skipped);
			}
			first = false;
			// `stats.rows` counts every row the cursor advanced over, the
			// pre-decode-filter rejects and the row a reload held included.
			yielded += stats.rows as usize;
			if stats.rows == 0 {
				break;
			}
			super::common::ensure_below_memory_threshold()?;
			// The records the batch read and refused before decoding spend the
			// VM's fuel here, since no row of theirs reaches it; once that
			// spends the fuel, the batch is handed on even when empty, so the
			// reader reaches its yield point between batches rather than after
			// the whole range. A record handed on is spent as its row runs.
			let refused = (stats.rows as usize).saturating_sub(decoded.len());
			let spent = crate::exec::vm::fuel::charge(crate::exec::vm::fuel::units(refused));
			if !decoded.is_empty() || spent {
				yielder
					.emit(if replay.is_some() {
						ValueBatch::stamped(decoded, stamps)
					} else {
						ValueBatch::new(decoded)
					})
					.await;
			}
		}
		Ok(())
	});

	Box::pin(stream)
}

/// What a scan does with one row its gate has admitted: the pre-decode filter,
/// the top-k probe, and the decode.
struct ScanRow<'a> {
	pdf: Option<&'a Arc<PreDecodeFilter>>,
	probe: Option<&'a Arc<TopKThresholdProbe>>,
	threshold: Option<&'a Value>,
	/// The layouts the row's gate admits against: a packed row is decoded through its
	/// generation's descriptor there.
	layouts: Option<&'a TableLayouts>,
	/// The table's migration steps, when it has any.
	replay: Option<&'a Replay>,
	/// What a row whose content is not an object yields.
	scalars: ScalarRows,
}

impl ScanRow<'_> {
	/// Appends the decoded row to `decoded` unless the filter or the probe
	/// rejects it from its stored bytes.
	///
	/// The caller has admitted `val`'s generation ([`RowGate::admit`]), so
	/// neither the filter nor the probe reads a field of a row the table's
	/// layouts do not cover. A row behind a migration step is judged by neither,
	/// since its stored bytes are not what a read of it sees. When the table has
	/// steps, the stamp of each decoded row is appended to `stamps`.
	#[inline]
	fn read(
		&self,
		key: &[u8],
		val: &[u8],
		decoded: &mut Vec<Value>,
		stamps: &mut Vec<Option<Generation>>,
		topk_skipped: &mut u64,
	) -> Result<(), ControlFlow> {
		let stamp = self.replay.map(|_| stamp_of(val));
		let judged = match (self.replay, stamp) {
			(Some(replay), Some(stamp)) => !replay.behind(stamp),
			_ => true,
		};
		// A packed row is checked once, for the filter and the probe together, and only where one
		// of them may read it and the row is judged at all.
		let admitted = if judged
			&& (self.pdf.is_some_and(|pdf| pdf.judges_packed_rows())
				|| self
					.probe
					.zip(self.threshold)
					.is_some_and(|(probe, _)| probe.reads_packed_rows()))
		{
			AdmittedRow::of(key, val, self.layouts)
		} else {
			AdmittedRow::Bytes(val)
		};
		if judged
			&& let Some(pdf) = self.pdf
			&& pdf.apply_in(&admitted, self.layouts) == PreDecodeFilterOutcome::Reject
		{
			// A rejected row still counts as scanned: the cursor read it.
			return Ok(());
		}
		// After the (cheaper, more selective) WHERE probe: skip decode when
		// the row's ORDER BY key provably cannot beat the downstream top-K
		// heap's worst entry.
		if judged
			&& let (Some(probe), Some(threshold)) = (self.probe, self.threshold)
			&& probe.rejects(threshold, &admitted)
		{
			*topk_skipped += 1;
			return Ok(());
		}
		// A packed row the filter or the probe checked is decoded from the checked row.
		let value = admitted.decode(key, val, self.layouts).map_err(ControlFlow::Err)?;
		decoded.push(match self.scalars {
			ScalarRows::Content => value,
			ScalarRows::RecordIds if !read_by_id(&value) => value,
			ScalarRows::RecordIds => {
				Value::RecordId(record_id_of_key(key).map_err(ControlFlow::Err)?)
			}
		});
		if let Some(stamp) = stamp {
			stamps.push(stamp);
		}
		Ok(())
	}
}

/// What a scan yields for a row whose stored content is not an object. Such a
/// record carries no `id` of its own, so the scan's only source of its record id
/// is its key.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScalarRows {
	/// The content as stored, for a read, which returns it as it is.
	Content,
	/// The record id the key names, for a write, which reads the record by that id
	/// as it reads any record it names. A row [`read_by_id`] passes over is
	/// yielded unchanged.
	RecordIds,
}

/// Whether a write reads the stored record `content` by its record id: content
/// that is neither an object nor `NONE`. `NONE` stays `NONE`, the value of a
/// record that is absent.
pub(crate) fn read_by_id(content: &Value) -> bool {
	!content.is_object() && !content.is_none()
}

/// The record id a storage key names.
pub(crate) fn record_id_of_key(key: &[u8]) -> anyhow::Result<RecordId> {
	let decoded_key =
		RecordKey::decode_key(key).map_err(|e| e.context("Failed to decode record key"))?;
	Ok(RecordId {
		table: decoded_key.tb.into_owned(),
		key: decoded_key.id.into_owned(),
	})
}

/// Decode a record from its key and value bytes.
///
/// The caller has admitted the row's generation against its table's layouts
/// ([`RowGate`], [`crate::kvs::LayoutSet`], or `Transaction::decode_record`)
/// and passes the layouts that admitted it: a packed row is read through its
/// generation's descriptor there, and is refused without them. The decode reads
/// the document or `revision` encoding, refuses an envelope this build does not
/// read, and does not check the generation of a document.
#[inline]
pub(crate) fn decode_record(
	key: &[u8],
	val: &[u8],
	layouts: Option<&TableLayouts>,
) -> Result<Value, ControlFlow> {
	decode_record_value(key, val, layouts).map_err(ControlFlow::Err)
}

/// [`decode_record`] with the failure as the typed error it is, for a caller
/// that holds no control flow: a refused envelope, or a packed row that its
/// layouts do not read, reaches the client as the [`crate::catalog::Error`] it
/// is.
#[inline]
pub(crate) fn decode_record_value(
	key: &[u8],
	val: &[u8],
	layouts: Option<&TableLayouts>,
) -> anyhow::Result<Value> {
	let decoded_key =
		RecordKey::decode_key(key).map_err(|e| e.context("Failed to decode record key"))?;

	let rid = crate::val::RecordId {
		table: decoded_key.tb.into_owned(),
		key: decoded_key.id.into_owned(),
	};

	let record = crate::catalog::Record::decode_admitted(val, layouts, rid)
		.map_err(|e| e.context("Failed to deserialize record"))?;

	// Take ownership of the value (zero-cost move for freshly deserialized data)
	Ok(record.data)
}

// =============================================================================
// Batch processing
// =============================================================================

/// Check if a value passes the permission check.
///
/// Inlined at each call site so the `Allow`/`Deny` branches are pure synchronous
/// code with zero async state-machine overhead. The `.await` only exists in the
/// `Conditional` arm.
macro_rules! check_perm {
	($permission:expr, $value:expr, $ctx:expr) => {
		match $permission {
			PhysicalPermission::Allow => Ok::<bool, ControlFlow>(true),
			PhysicalPermission::Deny => Ok(false),
			PhysicalPermission::Conditional(expr) => {
				// When already inside a permission predicate evaluation
				// (propagated via skip_fetch_perms), allow unconditionally
				// to prevent reentrant permission checks on cyclic links.
				if $ctx.root().skip_fetch_perms {
					Ok(true)
				} else {
					// A `select` guard refuses a write for the whole of its
					// evaluation, as `check_permission_for_value` does — see
					// the note there, and on why `$parent` needs the record
					// bound as the document root as well as the value. The
					// guard is definer code, so it runs in a privileged frame
					// that holds none of the caller's own bindings.
					let guarded = $ctx
						.clone()
						.privileged_frame()
						.refusing_writes(crate::dbs::NoWriteFrame::PermissionPredicate);
					let mut eval_ctx =
						EvalContext::from_exec_ctx(&guarded).with_value_and_doc($value);
					eval_ctx.skip_fetch_perms = true;
					expr.evaluate(eval_ctx).await.map(|v| v.is_truthy()).map_err(|e| {
						ControlFlow::Err(anyhow::anyhow!("Failed to check permission: {e}"))
					})
				}
			}
		}
	};
}

/// Combined single-pass filter and process for a batch of decoded values.
///
/// Per-record steps (sequential, in-place):
///   table permission -> [`materialise_fields_with_permissions`] -> WHERE predicate.
/// A table permission reading a computed field derives it on a copy of its own
/// ([`ComputedFieldGuard`](crate::exec::permission::ComputedFieldGuard)), so
/// the record reaches materialisation as stored.
/// Computed fields and field-level permissions both precede the predicate so a
/// field the reader may not see is already cut from the document the condition
/// is evaluated against, and a computed field derived from such a field already
/// reads as `NONE` there.
/// Records that fail any check are compacted out via an in-place swap so the
/// surviving prefix can be truncated at the end with no extra allocation.
///
/// Computed fields can make a row far wider than the record it was fetched
/// as, and a row the predicate rejects still holds them until that truncate,
/// so a batch that materialises computed fields is checked against
/// `SURREAL_MEMORY_THRESHOLD` (see
/// [`super::common::ensure_below_memory_threshold`]) before it is truncated,
/// while every materialised row is still held.
///
/// # Returns
///
/// The approximate size of the batch at that point, every row it held
/// included, when it materialised computed fields; 0 otherwise.
pub(crate) async fn filter_and_process_batch(
	batch: &mut Vec<Value>,
	permission: &PhysicalPermission,
	predicate: Option<&Clause>,
	ctx: &ExecutionContext,
	state: &FieldState,
	check_perms: bool,
) -> Result<usize, ControlFlow> {
	let needs_perm_filter = !matches!(permission, PhysicalPermission::Allow);

	// Fast path: when only the predicate is active (no permissions, no
	// computed fields), use evaluate_batch for potentially better throughput.
	if !needs_perm_filter
		&& state.computed_fields.is_empty()
		&& (!check_perms || state.field_permissions.is_empty())
		&& let Some(pred) = predicate
	{
		let eval_ctx = EvalContext::from_exec_ctx(ctx);
		let results = pred.evaluate_batch(eval_ctx, &batch[..]).await?;
		let mut write_idx = 0;
		for (read_idx, result) in results.into_iter().enumerate() {
			if result.is_truthy() {
				if write_idx != read_idx {
					batch.swap(write_idx, read_idx);
				}
				write_idx += 1;
			}
		}
		batch.truncate(write_idx);
		return Ok(0);
	}

	let mut write_idx = 0;
	for read_idx in 0..batch.len() {
		// Table-level permission (skip if Allow)
		if needs_perm_filter && !check_perm!(permission, &batch[read_idx], ctx)? {
			continue;
		}
		// Move to write position
		if write_idx != read_idx {
			batch.swap(write_idx, read_idx);
		}
		// Computed fields and field-level permissions, both of which must run
		// before the WHERE predicate so the condition is evaluated against the
		// document this caller is allowed to read.
		materialise_fields_with_permissions(ctx, state, &mut batch[write_idx], false, check_perms)
			.await?;
		// WHERE predicate (evaluated on the permission-reduced document)
		if let Some(pred) = predicate {
			let eval_ctx = EvalContext::from_exec_ctx(ctx).with_value_and_doc(&batch[write_idx]);
			if !pred.evaluate(eval_ctx).await?.is_truthy() {
				continue;
			}
		}
		write_idx += 1;
	}
	let materialised = if state.computed_fields.is_empty() {
		0
	} else {
		super::common::ensure_below_memory_threshold()?;
		batch.iter().map(super::common::approx_value_size).sum()
	};
	batch.truncate(write_idx);
	Ok(materialised)
}

// =============================================================================
// Field state
// =============================================================================

/// Cached state for field processing (computed fields and permissions).
/// Initialized on first batch and reused for subsequent batches.
///
/// `field_permissions` and `dep_map` are wrapped in `Arc` so that
/// [`filter_field_state_for_projection`] can share them across filtered
/// copies without cloning the underlying collection.
#[derive(Debug, Clone)]
pub(crate) struct FieldState {
	/// Computed field definitions converted to physical expressions
	pub(crate) computed_fields: Vec<ComputedFieldDef>,
	/// Field-level permissions, stored as `(idiom, perm)` pairs because the
	/// idiom may contain wildcards (`outer.*`, `items[*]`) that must be
	/// expanded against each value at evaluation time. Keyed lookup by
	/// flat field-name string is the wrong question — nested paths cannot
	/// be matched via top-level keys. See
	/// [`filter_fields_by_permission`] for the expansion logic.
	pub(crate) field_permissions: Arc<Vec<(crate::expr::Idiom, PhysicalPermission)>>,
	/// Dependency map for computed fields, used for projection filtering.
	/// Stored alongside the cached state so that projected queries can
	/// cheaply determine the subset of computed fields they need.
	dep_map: Arc<HashMap<String, crate::expr::computed_deps::ComputedDeps>>,
	/// Fields referenced by any conditional `PERMISSIONS FOR select WHERE …`
	/// expression on this table, which [`filter_field_state_for_projection`]
	/// adds to the projection-driven "needed" set. A guard judges the computed
	/// fields it reads on a view of its own ([`guard_view`]), so these are
	/// derived for the caller alone.
	/// `is_complete = false` (opaque expression) collapses into
	/// `permission_deps_complete = false`, which forces evaluation of all
	/// computed fields.
	permission_field_deps: Arc<HashSet<String>>,
	/// Whether `permission_field_deps` is exhaustive. False when any field
	/// permission expression contains opaque constructs (subqueries, params,
	/// etc.) that could reference fields outside of `permission_field_deps`.
	permission_deps_complete: bool,
	/// When each entry of `computed_fields` is derived relative to its select
	/// guard, aligned with `computed_fields`. `None` when every field is
	/// derived before its guard masks it ([`Derivation::AsMain`]), which is
	/// every field of a state built for a caller whose reads are not checked.
	/// [`Self::deferred_derivations`] is the only reader, and it answers `None`
	/// for a list that does not line up with `computed_fields`.
	derivations: Option<Arc<[PlannedDerivation]>>,
	/// The computed fields the conditional guards of `field_permissions` read,
	/// derived for them onto one copy of each record ([`guard_view`]). `None`
	/// when no guard reads one, and in a state whose reads are not checked.
	guard_reads: Option<Arc<GuardReads>>,
}

/// The `COMPUTED` fields of a table that its fields' conditional select
/// guards read, and the field state that derives them for the guards.
#[derive(Debug)]
pub(crate) struct GuardReads {
	table: TableName,
	/// The fields read by name, or `None` when a guard reads the row other than
	/// by name, which derives every computed field of the table.
	reads: Option<HashSet<String>>,
	/// The table's field state for reads that are not permission-checked,
	/// narrowed to the computed fields `reads` reaches. Resolved on first use
	/// and kept with the state that holds it, which is compiled once per table
	/// version; `None` once resolved to hold none.
	state: tokio::sync::OnceCell<Option<FieldState>>,
}

impl GuardReads {
	/// The field state that derives the guards' reads, or `None` when they
	/// reach no computed field. `frame` is the derivation's
	/// ([`guard_view`]), so the bodies are compiled for reads that are not
	/// permission-checked.
	async fn state(&self, frame: &ExecutionContext) -> Result<Option<&FieldState>, ControlFlow> {
		let state = self
			.state
			.get_or_try_init(|| async {
				let state =
					build_field_state(frame, &self.table, false, self.reads.as_ref()).await?;
				Ok::<_, ControlFlow>((!state.computed_fields.is_empty()).then_some(state))
			})
			.await?;
		Ok(state.as_ref())
	}
}

/// When one `COMPUTED` field is derived for a checked caller.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PlannedDerivation {
	/// The rule [`plan_table_derivations`] chose for the field.
	plan: Derivation,
	/// The index in [`FieldState::field_permissions`] of the guard a
	/// [`Derivation::JudgeFirst`] field is judged by. `None` for every other
	/// plan.
	guard: Option<usize>,
}

impl FieldState {
	/// Create an empty field state with no computed fields or field permissions.
	pub(crate) fn empty() -> Self {
		Self {
			computed_fields: Vec::new(),
			field_permissions: Arc::new(Vec::new()),
			dep_map: Arc::new(HashMap::new()),
			permission_field_deps: Arc::new(HashSet::new()),
			permission_deps_complete: true,
			derivations: None,
			guard_reads: None,
		}
	}

	/// The derivation plan of `computed_fields`, when some field is not
	/// derived before its guard. Aligned with `computed_fields`; `None` when
	/// every field is, or when the two do not correspond.
	fn deferred_derivations(&self) -> Option<&[PlannedDerivation]> {
		self.derivations.as_deref().filter(|plan| {
			!self.computed_fields.is_empty() && plan.len() == self.computed_fields.len()
		})
	}
}

/// A computed field definition ready for evaluation.
#[derive(Debug, Clone)]
pub(crate) struct ComputedFieldDef {
	/// The field name where to store the result
	field_name: String,
	/// The physical expression to evaluate
	expr: Clause,
	/// Optional type coercion
	kind: Option<crate::expr::Kind>,
	/// The definer's auth, recorded on the field by `DEFINE FIELD`. The body is
	/// evaluated under this so it cannot exercise more privilege than the
	/// identity that wrote it. `None` when the stamp cannot narrow anything
	/// (root Owner) cannot narrow anything, and a field with no stored stamp defaults to it,
	/// so the common case skips the context clone.
	auth_limit: Option<crate::iam::AuthLimit>,
	/// Whether the body holds a statement, a closure or a call of a
	/// user-defined function, through which deriving the field can derive it
	/// again: its derivation spends a unit of the computation-depth budget.
	reenters: bool,
}

impl ComputedFieldDef {
	/// Test-only constructor: production definitions are built exclusively by
	/// [`build_field_state_raw`] from catalog field definitions.
	#[cfg(test)]
	pub(crate) fn for_test(field_name: impl Into<String>) -> Self {
		Self {
			field_name: field_name.into(),
			expr: Clause::literal(Value::None),
			kind: None,
			auth_limit: None,
			reenters: false,
		}
	}
}

/// Convert a definition's stored `AUTH LIMIT` into the narrowing to apply to
/// its bodies, or `None` when the stamp cannot narrow any caller.
///
/// A root-`Owner` stamp is inert: `Level::Root` is a sublevel of nothing but
/// `Level::Root`, so the caller keeps its own level, and retaining roles `<=
/// Owner` keeps every role. A field with no stored stamp defaults to this stamp,
/// so this is also the most common case.
pub(crate) fn narrowing_auth_limit(
	auth_limit: &crate::catalog::auth::AuthLimit,
) -> Result<Option<crate::iam::AuthLimit>, anyhow::Error> {
	if auth_limit == &crate::catalog::auth::AuthLimit::new_no_limit() {
		return Ok(None);
	}
	Ok(Some(crate::iam::AuthLimit::try_from(auth_limit)?))
}

/// The fields of this table whose read is not inert, or `None` when no field
/// read of it is.
///
/// Materialising a row runs the table's `SELECT` permission, then the computed
/// fields and field-level permissions the query needs, all before a condition
/// observes the row. A fold that discards a field read discards whatever that
/// read would have triggered — including a call the caller is not permitted to
/// make.
///
/// A conditional table permission is evaluated for any row that is read at all,
/// so it makes every field read non-inert and answers `None`. Otherwise only
/// the fields that carry work of their own are listed, since materialisation
/// resolves the fields the query needs rather than all of them.
///
/// `check_perms` mirrors the enforcement decision the scan itself makes: a
/// session that skips permission evaluation never runs those expressions, so
/// they are not work this fold would be discarding. Computed fields are
/// evaluated either way, and are the only per-name entries — materialisation
/// resolves those from the fields the query needs.
///
/// Answers `None` whenever the definitions cannot be read or a field is named
/// by something other than a plain path, so an unresolvable table costs an
/// optimisation rather than an enforcement.
pub(crate) async fn table_read_restricted_fields(
	txn: &Transaction,
	ns_id: crate::catalog::NamespaceId,
	db_id: crate::catalog::DatabaseId,
	table_name: &TableName,
	version: Option<u64>,
	check_perms: bool,
) -> Option<HashSet<String>> {
	let Ok(table_def) = txn.get_tb(ns_id, db_id, table_name, version).await else {
		return None;
	};
	// `Full` evaluates nothing and `None` withholds the rows outright; only a
	// conditional permission runs an expression, and only when enforcement is
	// on for this session at all.
	if check_perms
		&& matches!(
			crate::catalog::table_select_permission(table_def.as_deref()),
			Permission::Specific(_)
		) {
		return None;
	}

	let Ok(field_defs) = txn.all_tb_fields(ns_id, db_id, table_name, version).await else {
		return None;
	};
	// A conditional field permission is enforced for every restricted field on
	// the table whatever the query touches — `filter_field_state_for_projection`
	// keeps the whole list on purpose — so it is row-wide work, not work
	// attached to reads of its own name.
	if check_perms
		&& field_defs.iter().any(|fd| matches!(fd.select_permission, Permission::Specific(_)))
	{
		return None;
	}

	let mut restricted = HashSet::new();
	for fd in field_defs.iter() {
		if fd.computed.is_none() {
			continue;
		}
		// A definition on `a.b` is reached by reading `a`, so the read that
		// has to be kept is named by the leading segment.
		match fd.name.0.first() {
			Some(crate::expr::part::Part::Field(name)) => {
				restricted.insert(name.to_string());
			}
			_ => return None,
		}
	}
	Some(restricted)
}

/// The field state of reads of `table_name` whose fields are `field_defs`,
/// compiled by `planner`, with field permissions where `check_perms`.
pub(crate) async fn compile_field_state(
	planner: &crate::exec::planner::Planner<'_>,
	table_name: &TableName,
	field_defs: &[crate::catalog::FieldDefinition],
	check_perms: bool,
) -> Result<FieldState, ControlFlow> {
	// Fast path: if there are no computed fields and no field-level permissions
	// that need checking, skip the expensive resolution. Both Permission::None
	// (deny) and Permission::Specific (conditional) require enforcement.
	let has_computed = field_defs.iter().any(|fd| fd.computed.is_some());
	let has_field_perms = check_perms
		&& field_defs.iter().any(|fd| !matches!(fd.select_permission, Permission::Full));
	if !has_computed && !has_field_perms {
		return Ok(FieldState::empty());
	}

	// Computed-field and permission expressions are compiled through the
	// supplied planner. When the planner has a transaction (plan-time path),
	// inner subqueries benefit from plan-time index resolution; the
	// planner's `CycleGuard` prevents recursive table-resolution for
	// self-referential permissions like
	// `WHERE (SELECT FROM same_table) != NONE`. When the planner is txn-less
	// (runtime resolution), inner subqueries compile to runtime-resolving scans.

	// Collect ALL computed fields and their dependency metadata.
	let mut raw_computed: Vec<RawComputedField> = Vec::new();
	let mut dep_map: HashMap<String, crate::expr::computed_deps::ComputedDeps> = HashMap::new();

	for fd in field_defs.iter() {
		// The dependency analysis reads the stored clause; the body the engine
		// evaluates is its logical form, which fails when evaluated if the
		// clause has none.
		if let (Some(expr), Some(body)) = (fd.computed.as_ref(), fd.computed_logical()) {
			// The key the record holds the field under, which is the name every
			// other clause reads it by.
			let field_name = fd
				.name
				.field_name()
				.ok_or_else(|| {
					ControlFlow::Err(anyhow::anyhow!(
						"a computed field is not named by one field: {}",
						fd.name.to_raw_string()
					))
				})?
				.to_owned();

			let deps = crate::expr::computed_deps::extract_computed_deps(expr);

			dep_map.insert(field_name.clone(), deps.clone());

			// The field is computed for the principal its `AUTH LIMIT` narrows
			// to, so the statements it holds are planned for that principal.
			let limit = narrowing_auth_limit(&fd.auth_limit)?;
			let limited = match (&limit, &planner.auth) {
				(Some(limit), Some(_)) => Some(planner.limited(limit)),
				_ => None,
			};
			let body_planner = limited.as_ref().unwrap_or(planner);
			let (clause, reenters) = match body {
				Ok(body) => {
					let compiled =
						body_planner.scalar_metadata(body.clone()).await.with_context(|| {
							format!("Computed field '{field_name}' has unsupported expression")
						})?;
					let reenters = crate::exec::ir::logical::stages::needs_caller(body);
					(Clause::of(body.clone(), compiled), reenters)
				}
				Err(reason) => (Clause::uncompilable(reason.to_string()), false),
			};

			raw_computed.push((
				field_name,
				clause,
				fd.field_kind.clone(),
				deps.fields,
				limit,
				reenters,
			));
		}
	}

	// Topologically sort ALL computed fields for correct evaluation order
	let topo_input: Vec<(String, Vec<String>)> =
		raw_computed.iter().map(|(name, _, _, deps, _, _)| (name.clone(), deps.clone())).collect();
	let sorted_indices = crate::expr::computed_deps::topological_sort_computed_fields(&topo_input);

	let mut computed_fields = Vec::with_capacity(sorted_indices.len());
	for idx in sorted_indices {
		let (field_name, expr, kind, _, auth_limit, reenters) = &raw_computed[idx];
		computed_fields.push(ComputedFieldDef {
			field_name: field_name.clone(),
			expr: expr.clone(),
			kind: kind.clone(),
			auth_limit: auth_limit.clone(),
			reenters: *reenters,
		});
	}

	// Build field permissions, preserving each field's original Idiom so
	// `filter_fields_by_permission` can expand wildcards via `Value::each`.
	// `Permission::Full` entries are skipped — they're "always allow" and
	// don't need a runtime check.
	//
	// While walking conditional permissions, accumulate the set of fields
	// the expression references, which `filter_field_state_for_projection`
	// adds to the projection-driven "needed" set, and what the guards read
	// of the row by name, which `guard_view` derives for them.
	let mut field_permissions: Vec<(crate::expr::Idiom, PhysicalPermission)> = Vec::new();
	let mut permission_field_deps: HashSet<String> = HashSet::new();
	let mut permission_deps_complete = true;
	// What the guards read of the row by name, `$value` being bound to the
	// field's value: `None` once one reads the row other than by name.
	let mut guard_reads: Option<HashSet<String>> = Some(HashSet::new());
	if check_perms {
		for fd in field_defs.iter() {
			let physical_perm = match &fd.select_permission {
				Permission::Full => continue,
				Permission::None => PhysicalPermission::Deny,
				Permission::Specific(expr) => {
					let reads =
						crate::expr::computed_deps::extract_computed_deps_binding(expr, &["value"]);
					match (&mut guard_reads, reads.is_complete) {
						(Some(all), true) => all.extend(reads.fields),
						_ => guard_reads = None,
					}
					let deps = crate::expr::computed_deps::extract_computed_deps(expr);
					if !deps.is_complete {
						// Read the flag *before* flipping it below so we only
						// emit on the first opaque field — one log line per
						// table build. `FieldState` is cached per `(table,
						// check_perms)`, so this fires once per distinct table
						// per cache lifetime.
						if permission_deps_complete {
							crate::expr::computed_deps::warn_incomplete_perm_deps(
								table_name.as_str(),
								fd.name.to_raw_string().as_str(),
							);
						}
						permission_deps_complete = false;
					}
					permission_field_deps.extend(deps.fields);
					// The guard in the logical form the catalog compiled from
					// its text.
					read_permission_to_physical(
						&fd.select_permission,
						fd.select_permission_logical(),
						GuardKind::Read,
						planner,
					)
					.await
					.context("Failed to convert field permission")?
				}
			};
			field_permissions.push((fd.name.clone(), physical_perm));
		}
	}

	let derivations = if check_perms {
		plan_field_derivations(field_defs, &computed_fields, &field_permissions)
	} else {
		None
	};
	// The guards judge the computed fields they read as derived in their own
	// frame, on one copy of the record ([`guard_view`]).
	let guard_reads = if check_perms && !dep_map.is_empty() {
		match guard_reads {
			None => Some(None),
			Some(reads) => {
				let reads: HashSet<String> =
					reads.into_iter().filter(|name| dep_map.contains_key(name)).collect();
				(!reads.is_empty()).then_some(Some(reads))
			}
		}
	} else {
		None
	};
	let guard_reads = guard_reads.map(|reads| {
		Arc::new(GuardReads {
			table: table_name.clone(),
			reads,
			state: tokio::sync::OnceCell::new(),
		})
	});

	Ok(FieldState {
		computed_fields,
		field_permissions: Arc::new(field_permissions),
		dep_map: Arc::new(dep_map),
		permission_field_deps: Arc::new(permission_field_deps),
		permission_deps_complete,
		derivations,
		guard_reads,
	})
}

/// The derivation plan of `computed_fields` for a caller whose reads are
/// checked, or `None` when every field is derived before its guard.
///
/// The decision is [`plan_table_derivations`] over the whole table's
/// definitions, matched to `computed_fields` by the name the record holds the
/// field under. A [`Derivation::JudgeFirst`] field whose own guard is not in
/// `field_permissions` has no guard to judge by, so it is derived before its
/// guard like any other. A judged or hidden field's own guard entry is kept in
/// the plan, so that the computed permission pass skips it.
fn plan_field_derivations(
	field_defs: &[crate::catalog::FieldDefinition],
	computed_fields: &[ComputedFieldDef],
	field_permissions: &[(crate::expr::Idiom, PhysicalPermission)],
) -> Option<Arc<[PlannedDerivation]>> {
	let decided = plan_table_derivations(field_defs, true);
	let planned: Vec<PlannedDerivation> = computed_fields
		.iter()
		.map(|cf| {
			let as_main = PlannedDerivation {
				plan: Derivation::AsMain,
				guard: None,
			};
			let Some(definition) = field_defs
				.iter()
				.find(|fd| fd.computed.is_some() && fd.name.field_name() == Some(&cf.field_name))
			else {
				return as_main;
			};
			let key = definition.name.to_raw_string();
			let guard = field_permissions.iter().position(|(idiom, _)| *idiom == definition.name);
			match decided.iter().find(|(name, _)| *name == key).map(|(_, plan)| *plan) {
				// A guard inherited from an ancestor field has no entry of its own.
				Some(Derivation::Hidden) => PlannedDerivation {
					plan: Derivation::Hidden,
					guard,
				},
				Some(Derivation::JudgeFirst) if guard.is_some() => PlannedDerivation {
					plan: Derivation::JudgeFirst,
					guard,
				},
				Some(Derivation::JudgeFirst | Derivation::AsMain) | None => as_main,
			}
		})
		.collect();
	planned.iter().any(|p| p.plan != Derivation::AsMain).then(|| Arc::from(planned))
}

/// The field state of reads of `table_name` in `ctx`, with field permissions
/// where `check_perms`, narrowed to `needed_fields` where it is `Some`.
///
/// The full state is the table descriptor's
/// ([`crate::exec::vm::descriptor::field_state`]), compiled once per table
/// version, so narrowing it per projection is the only work a call does.
pub(crate) async fn build_field_state(
	ctx: &ExecutionContext,
	table_name: &TableName,
	check_perms: bool,
	needed_fields: Option<&std::collections::HashSet<String>>,
) -> Result<FieldState, ControlFlow> {
	let full = crate::exec::vm::descriptor::field_state(ctx, table_name, check_perms).await?;
	Ok(filter_field_state_for_projection(&full, needed_fields))
}

/// Filter a full FieldState down to only the computed fields required by
/// the given projection. When `needed_fields` is None (SELECT *), returns
/// a clone of the full state. This is a cheap CPU-only operation with no
/// KV lookups.
///
/// Even when the projection is selective, computed fields referenced by any
/// conditional `PERMISSIONS FOR select WHERE …` expression on this table are
/// evaluated for the caller, all of them where an expression has opaque
/// dependencies (subqueries, params). The guards judge the computed fields
/// they read on a view of their own ([`guard_view`]), not as evaluated here.
pub(crate) fn filter_field_state_for_projection(
	full_state: &FieldState,
	needed_fields: Option<&std::collections::HashSet<String>>,
) -> FieldState {
	let Some(needed) = needed_fields else {
		return full_state.clone();
	};

	if !full_state.permission_deps_complete {
		// A permission expression contains opaque constructs, so we cannot
		// statically determine which computed fields it might reference.
		// Evaluate them all.
		return full_state.clone();
	}

	// Union the projection's needed fields with the set of fields referenced
	// by any conditional field-permission expression.
	let mut needed_with_perms: std::collections::HashSet<String> = needed.clone();
	needed_with_perms.extend(full_state.permission_field_deps.iter().cloned());

	let required = crate::expr::computed_deps::resolve_required_computed_fields(
		&needed_with_perms,
		&full_state.dep_map,
	);

	// The derivation plan is decided over the whole table, so narrowing the
	// projection keeps each surviving field's entry and never makes a field
	// eligible that the table's plan did not.
	let kept: Option<Vec<usize>> = required.as_ref().map(|required_set| {
		(0..full_state.computed_fields.len())
			.filter(|&i| required_set.contains(&full_state.computed_fields[i].field_name))
			.collect()
	});
	let (computed_fields, derivations) = match kept {
		Some(kept) => {
			let derivations = full_state
				.deferred_derivations()
				.map(|plan| kept.iter().map(|&i| plan[i]).collect::<Arc<[PlannedDerivation]>>());
			let computed_fields =
				kept.iter().map(|&i| full_state.computed_fields[i].clone()).collect();
			(computed_fields, derivations)
		}
		None => (full_state.computed_fields.clone(), full_state.derivations.clone()),
	};
	let derivations = derivations.filter(|plan| plan.iter().any(|p| p.plan != Derivation::AsMain));

	FieldState {
		computed_fields,
		derivations,
		// SECURITY: `field_permissions` is retained in full here — it is
		// deliberately NOT filtered down to the projection. Field-level SELECT
		// permissions must be enforced for every restricted field on the table
		// regardless of whether it appears in the projection (a restricted
		// field can be referenced only by WHERE/ORDER BY). The value-ordering
		// guard in `operators/scan/dynamic.rs`
		// (`order_touches_restricted_select_field`) also reads this list to
		// decide whether an `ORDER BY` targets a restricted field, so filtering
		// it by projection would let `SELECT id ... ORDER BY <restricted>`
		// (where the field is sorted on but not projected) slip past the guard
		// and re-open the value-ordering oracle.
		field_permissions: Arc::clone(&full_state.field_permissions),
		dep_map: Arc::clone(&full_state.dep_map),
		permission_field_deps: Arc::clone(&full_state.permission_field_deps),
		permission_deps_complete: full_state.permission_deps_complete,
		guard_reads: full_state.guard_reads.clone(),
	}
}

/// Compute all computed fields for a single value.
///
/// When `skip_fetch_perms` is `true`, any RecordId dereferences inside
/// computed field expressions will bypass permission checks (using
/// `fetch_record_no_perms`).  This must be set when computing fields
/// during permission predicate evaluation to prevent reentrant permission
/// checks on cyclic record links.
pub(crate) async fn compute_fields_for_value(
	ctx: &ExecutionContext,
	state: &FieldState,
	value: &mut Value,
	skip_fetch_perms: bool,
) -> Result<(), ControlFlow> {
	let view = shadowed_stored_view(state, value);
	compute_fields_for_value_against(ctx, state, value, skip_fetch_perms, view).await
}

/// The view a self-dereference resolves against when the record carries a key
/// shadowed by a COMPUTED field of the same name, or `None` when it carries
/// none.
///
/// A key written while a field was an ordinary field stays in the record's bytes
/// after that field is redefined as COMPUTED. The computed field does not reach it, but
/// self-dereference reads storage directly and would return it. Dropping the
/// computed names makes `id.<name>` yield `NONE` there — the answer a read of
/// storage gives for a name that is not stored.
///
/// Derived here rather than at the call sites so that every path evaluating
/// computed fields resolves a self-dereference the same way, including
/// permission-predicate evaluation, which computes without applying
/// permissions and so never reaches the passes in
/// [`materialise_fields_with_permissions`].
///
/// Sampled before the computed fields are written: afterwards every computed
/// name is present by construction, so the test would match on every table
/// that has one.
fn shadowed_stored_view(state: &FieldState, value: &Value) -> Option<Value> {
	let shadowed = if let Value::Object(obj) = value {
		state.dep_map.keys().any(|name| obj.contains_key(name.as_str()))
	} else {
		false
	};
	shadowed.then(|| stored_fields_view(state, value))
}

/// [`compute_fields_for_value`] with the view a self-dereference resolves
/// against supplied explicitly.
///
/// `stored_view` becomes [`ComputingRecord::stored`]: the row's stored fields
/// as the caller may read them. A caller that has reduced `value` passes one,
/// so a computed field spelled `id.<field>` reads the reduced row instead of
/// storage and cannot recover a field the reduction removed. `None` keeps the
/// read from storage, which is what a caller holding no reduced view wants.
pub(crate) async fn compute_fields_for_value_against(
	ctx: &ExecutionContext,
	state: &FieldState,
	value: &mut Value,
	skip_fetch_perms: bool,
	stored_view: Option<Value>,
) -> Result<(), ControlFlow> {
	derive_fields(ctx, state, value, skip_fetch_perms, stored_view, None).await
}

/// [`compute_fields_for_value_against`] deriving only the fields `verdicts`
/// admits: `verdicts[i]` is whether `state.computed_fields[i]` is derived, and
/// `None` derives them all. A field that is not derived leaves the row as it
/// is.
async fn derive_fields(
	ctx: &ExecutionContext,
	state: &FieldState,
	value: &mut Value,
	skip_fetch_perms: bool,
	stored_view: Option<Value>,
	verdicts: Option<&[bool]>,
) -> Result<(), ControlFlow> {
	if state.computed_fields.is_empty() {
		return Ok(());
	}

	let mut eval_ctx = EvalContext::from_exec_ctx(ctx);
	eval_ctx.skip_fetch_perms = skip_fetch_perms;

	// Extract the record ID before entering the loop so that field
	// dereferences that target this same record are answered directly
	// instead of re-computing fields (which would loop forever).
	eval_ctx.computing_record = if let Value::Object(obj) = &*value {
		if let Some(Value::RecordId(rid)) = obj.get("id") {
			Some(Arc::new(crate::exec::eval_context::ComputingRecord {
				rid: rid.clone(),
				stored: stored_view,
			}))
		} else {
			None
		}
	} else {
		None
	};

	// SECURITY: the body is definer code, so it runs in a privileged frame that
	// holds none of the caller's bindings ([`ExecutionContext::privileged_frame`]),
	// and the frame refuses writes, whichever auth it holds.
	// `Expr::contains_mutation` rejects a mutation written into the body at
	// definition time but cannot see through a call to a user-defined function,
	// so without the refusal a read of the field would mutate the database as
	// the definer. Loop-invariant, so derived once per row rather than per field.
	let field_ctx = ctx.clone().privileged_frame().computing_field();
	// A body that can derive the fields of a record again — through a statement reading the
	// table, a closure or a function — spends a unit of the computation-depth budget for the row,
	// so a cycle of derivations ends in the computation-depth error, as recursion through a
	// function does.
	let reenters = state
		.computed_fields
		.iter()
		.enumerate()
		.any(|(index, cf)| cf.reenters && verdicts.is_none_or(|verdicts| verdicts[index]));
	let field_ctx = match reenters {
		true => field_ctx.dive().map_err(ControlFlow::Err)?,
		false => field_ctx,
	};

	for (index, cf) in state.computed_fields.iter().enumerate() {
		if verdicts.is_some_and(|verdicts| !verdicts[index]) {
			continue;
		}
		// SECURITY: apply the field's AUTH LIMIT so the body runs under the
		// definer's auth, not the reader's. Without it a low-privileged definer
		// can plant a body that a high-privileged reader then executes with
		// their own privilege. Mirrors `Document::computed_fields_inner`.
		//
		// The narrowed context is derived per field rather than hoisted because
		// each field carries its own stamp; `narrowing_auth_limit` returns `None`
		// for the inert root-Owner stamp so the common case pays nothing.
		let limited_ctx = cf.auth_limit.as_ref().map(|limit| field_ctx.with_limited_auth(limit));
		let narrowed_eval_ctx = limited_ctx.as_ref().unwrap_or(&field_ctx);
		let narrowed_eval_ctx = {
			let mut narrowed = EvalContext::from_exec_ctx(narrowed_eval_ctx);
			narrowed.skip_fetch_perms = skip_fetch_perms;
			narrowed.computing_record.clone_from(&eval_ctx.computing_record);
			narrowed
		};
		// Evaluate with the row as both current value and document root so
		// nested subqueries see the same `$parent` as top-level projections.
		let row_ctx = narrowed_eval_ctx.with_value_and_doc(value);
		let computed_value = match cf.expr.evaluate(row_ctx).await {
			Ok(v) => v,
			Err(ControlFlow::Return(v)) => v,
			Err(e) => return Err(e),
		};

		// A value that does not coerce to the field's kind, or that a named type the kind holds
		// does not admit, fails naming the computed field.
		let final_value = if let Some(kind) = &cf.kind {
			let refused = |error| {
				ControlFlow::Err(
					anyhow::Error::new(error)
						.context(format!("Failed to coerce computed field '{}'", cf.field_name)),
				)
			};
			let coerced = computed_value.coerce_to_kind(kind).map_err(refused)?;
			crate::exec::vm::schema_host::check_types_held(&field_ctx, kind, coerced)
				.await
				.map_err(refused)?
		} else {
			computed_value
		};

		// Inject the computed value into the document
		if let Value::Object(obj) = value {
			obj.insert(cf.field_name.clone(), final_value);
		} else {
			return Err(ControlFlow::Err(anyhow::anyhow!("Value is not an object: {:?}", value)));
		}
	}

	Ok(())
}

/// The stored fields of `value`, as the caller may read them.
///
/// `value` is the reduced row after the stored permission pass, still carrying
/// the computed fields the first evaluation left on it. Dropping every COMPUTED
/// name yields what a read of storage would return for this caller — the shape
/// a self-dereference has always seen, minus the fields the reduction removed.
///
/// `dep_map` is keyed by every COMPUTED field on the table and, unlike
/// `computed_fields`, is not narrowed by the projection, so a state built for a
/// selective `SELECT` still drops all of them.
fn stored_fields_view(state: &FieldState, value: &Value) -> Value {
	let mut view = value.clone();
	if let Value::Object(obj) = &mut view {
		for name in state.dep_map.keys() {
			obj.remove(name.as_str());
		}
	}
	view
}

/// Which of `state.field_permissions` a pass applies.
///
/// The two-pass materialisation splits the entries by field kind, for this
/// reason: a field's clause must be answered
/// exactly once, against the view that field actually exists on. Evaluating one
/// twice and keeping the union is not merely wasted work — a volatile predicate
/// such as `WHERE rand::bool()` would be allowed by either roll, turning a 50%
/// policy into 75%.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PermissionPass {
	/// Every entry, for the single-pass case.
	All,
	/// Only fields with no `COMPUTED` clause, mirroring `reduce_document`.
	/// These are the cuts that make re-derivation produce a different answer.
	Stored,
	/// Only `COMPUTED` fields, mirroring `filter_computed_field_permissions`.
	/// Deferred to after re-derivation, which is where their values are final.
	Computed,
}

impl PermissionPass {
	/// Whether this pass applies the entry for `idiom`.
	///
	/// `dep_map` is keyed by every `COMPUTED` field on the table and is *not*
	/// narrowed by the projection, unlike `computed_fields`, so it stays a
	/// sound classifier for a state built for a selective `SELECT`.
	fn applies_to(self, state: &FieldState, idiom: &crate::expr::Idiom) -> bool {
		let computed = match idiom.0.as_slice() {
			[crate::expr::Part::Field(name)] => state.dep_map.contains_key(name.as_str()),
			// A computed field is top-level, so only a single-segment path can name one.
			_ => false,
		};
		match self {
			PermissionPass::All => true,
			PermissionPass::Stored => !computed,
			PermissionPass::Computed => computed,
		}
	}
}

/// Populate `value`'s computed fields and apply field-level permissions, in the
/// order that stops a computed field from carrying a value the caller's
/// permissions deny.
///
/// Computed fields are evaluated first, as the caller reads them: a computed
/// field's own clause judges the value the caller would receive. A clause that
/// reads a computed field judges it as derived in the clause's own frame, on
/// one view of the record shared by every clause ([`guard_view`]). The first
/// evaluation leaves every computed field derived from the document *before*
/// any field was cut, so one whose body reads a denied field still holds its
/// value once the permission pass has removed the source. Re-deriving them
/// against the reduced document is what keeps the record consistent.
///
/// The passes are split by field kind, as [`PermissionPass`] describes: stored
/// fields are cut first, because those cuts are what re-derivation reacts to,
/// and a computed field's own clause is applied afterwards, against its final
/// value. Every clause is therefore evaluated exactly once.
///
/// Re-derivation itself is skipped whenever it cannot change the answer: the
/// table has no computed fields, no field on it restricts reads, or the stored
/// pass cut nothing.
///
/// A computed field that reaches its own row by dereferencing the record id
/// (`COMPUTED id.secret`) is served by [`crate::exec::vm::access::field`] from the
/// view supplied here rather than from storage, so the reduction reaches it
/// too.
pub(crate) async fn materialise_fields_with_permissions(
	ctx: &ExecutionContext,
	state: &FieldState,
	value: &mut Value,
	skip_fetch_perms: bool,
	check_perms: bool,
) -> Result<(), ControlFlow> {
	if check_perms
		&& !state.field_permissions.is_empty()
		&& let Some(plan) = state.deferred_derivations()
	{
		return materialise_judged(ctx, state, plan, value, skip_fetch_perms).await;
	}
	compute_fields_for_value(ctx, state, value, skip_fetch_perms).await?;
	// Nothing on the table restricts reads, so nothing can be cut and nothing
	// needs re-deriving. Checked before the snapshot below so a table whose
	// fields are all readable pays no clone.
	if !check_perms || state.field_permissions.is_empty() {
		return Ok(());
	}
	// Without computed fields there is nothing to re-derive and nothing to
	// defer, so one pass over every entry is both necessary and sufficient —
	// and it snapshots lazily.
	if state.computed_fields.is_empty() {
		return filter_fields_by_permission(ctx, state, value).await;
	}
	// The record as computed from storage. Both passes answer their predicates
	// against it rather than against `value`: `value` has the caller's denied
	// fields cut from it, so a clause naming one would read it as `NONE` there
	// and pass a field the clause was written to deny.
	let record = value.clone();
	// The computed values and their snapshot are both in place, and the passes
	// below may cut them before the batch's own threshold check would see them.
	super::common::ensure_below_memory_threshold()?;
	// What both passes judge: the record, with the computed fields the guards
	// read derived in their own frame where they read one.
	let view = guard_view(ctx, state, &record).await?;
	// Stored fields first. `value` still equals `record`, so it doubles as the
	// snapshot that `each` expansion and the `$value` binding read.
	let cut = filter_fields_by_permission_inner(
		ctx,
		state,
		value,
		PermissionPass::Stored,
		Some(&record),
		Some(view.as_deref().unwrap_or(&record)),
		&[],
	)
	.await?;
	if cut {
		// Re-derive the computed fields against the reduced record, so one
		// whose body reads a denied field yields `NONE` instead of the value it
		// held before that field was cut. The same reduced row backs the
		// self-dereference, so `id.<field>` reads neither the cut value nor a
		// shadowed stored one back out of storage.
		let stored_view = stored_fields_view(state, value);
		compute_fields_for_value_against(ctx, state, value, skip_fetch_perms, Some(stored_view))
			.await?;
		// The re-derived values are in place, and the computed fields' own
		// clauses below may cut them.
		super::common::ensure_below_memory_threshold()?;
	}
	// The computed fields' own clauses, against their final values. The
	// snapshot is left implicit when a re-derivation ran, because `each` and
	// `$value` must read the re-derived view — the value the caller would
	// receive, not the pre-reduction one. With no re-derivation `value` still
	// equals `record`, which is that view and saves the clone.
	let snapshot = (!cut).then_some(&record);
	filter_fields_by_permission_inner(
		ctx,
		state,
		value,
		PermissionPass::Computed,
		snapshot,
		Some(view.as_deref().unwrap_or(&record)),
		&[],
	)
	.await?;
	Ok(())
}

/// [`materialise_fields_with_permissions`] for a table where some `COMPUTED`
/// field is not derived before its select guard: `plan` is
/// [`FieldState::deferred_derivations`].
///
/// A per-row verdict decides which fields are derived at all, taken against the
/// row as stored before anything is derived onto it: a [`Derivation::Hidden`]
/// field is never derived, a [`Derivation::JudgeFirst`] field is derived only
/// when its guard admits the stored row, and every other field is derived. A
/// field that is not derived is absent from the result, including a stored key
/// of the same name that it shadows. The later passes then run as they do for
/// any table, except that the computed pass skips every guard already judged,
/// so each guard answers once per row and a volatile one cannot roll twice.
/// The re-derivation after the stored pass derives the same fields and no
/// others.
async fn materialise_judged(
	ctx: &ExecutionContext,
	state: &FieldState,
	plan: &[PlannedDerivation],
	value: &mut Value,
	skip_fetch_perms: bool,
) -> Result<(), ControlFlow> {
	// A guard that errors fails the row with its error, unchanged, so a retryable
	// conflict stays downcastable; nothing is derived on a guess.
	let mut verdicts: SmallVec<[bool; 8]> = SmallVec::with_capacity(plan.len());
	let mut judged: SmallVec<[usize; 8]> = SmallVec::new();
	for planned in plan {
		let derive = match (planned.plan, planned.guard) {
			(Derivation::AsMain, _) => true,
			(Derivation::Hidden, guard) => {
				judged.extend(guard);
				false
			}
			(Derivation::JudgeFirst, Some(guard)) => {
				judged.push(guard);
				let (_, permission) = &state.field_permissions[guard];
				judge_before_derivation(permission, value, ctx).await.map_err(ControlFlow::Err)?
			}
			// A judged field with no guard to judge it by is derived unconditionally.
			(Derivation::JudgeFirst, None) => true,
		};
		verdicts.push(derive);
	}
	// Sampled before any key is removed, for the reason given on
	// `shadowed_stored_view`.
	let view = shadowed_stored_view(state, value);
	if let Value::Object(obj) = &mut *value {
		for (cf, _) in state.computed_fields.iter().zip(&verdicts).filter(|(_, derive)| !**derive) {
			obj.remove(cf.field_name.as_str());
		}
	}
	derive_fields(ctx, state, value, skip_fetch_perms, view, Some(&verdicts)).await?;

	// The record as derived from storage, for the passes' predicates; see
	// `materialise_fields_with_permissions`.
	let record = value.clone();
	super::common::ensure_below_memory_threshold()?;
	let view = guard_view(ctx, state, &record).await?;
	let cut = filter_fields_by_permission_inner(
		ctx,
		state,
		value,
		PermissionPass::Stored,
		Some(&record),
		Some(view.as_deref().unwrap_or(&record)),
		&judged,
	)
	.await?;
	if cut {
		let stored_view = stored_fields_view(state, value);
		derive_fields(ctx, state, value, skip_fetch_perms, Some(stored_view), Some(&verdicts))
			.await?;
		super::common::ensure_below_memory_threshold()?;
	}
	let snapshot = (!cut).then_some(&record);
	filter_fields_by_permission_inner(
		ctx,
		state,
		value,
		PermissionPass::Computed,
		snapshot,
		Some(view.as_deref().unwrap_or(&record)),
		&judged,
	)
	.await?;
	Ok(())
}

/// Filter fields from a value based on field-level permissions.
///
/// Each `(idiom, perm)` entry is expanded via [`Value::each`] to handle
/// wildcards (`outer.*`, `items[*]`), then each concrete path is checked
/// with `$value` bound to the picked field value, so a permission on a nested
/// or wildcard path judges the field it names.
pub(crate) async fn filter_fields_by_permission(
	ctx: &ExecutionContext,
	state: &FieldState,
	value: &mut Value,
) -> Result<(), ControlFlow> {
	let view = guard_view(ctx, state, value).await?;
	filter_fields_by_permission_inner(
		ctx,
		state,
		value,
		PermissionPass::All,
		None,
		view.as_deref(),
		&[],
	)
	.await?;
	Ok(())
}

/// `record` as the conditional field guards of `state` judge it: a copy with
/// the computed fields they read, and those those read in turn, derived onto
/// it in dependency order in the frame a permission predicate derives them in
/// — reads are not permission-checked, writes are refused, and each body runs
/// under its field's `AUTH LIMIT` — or `None` where no guard reads a computed
/// field and the guards judge `record` itself.
///
/// Derived as the caller, a body reading a record the caller may not select
/// reads `NONE`, which satisfies a guard written to deny; the guard reads what
/// its body would read written there in its place, as a table's guard does
/// ([`ComputedFieldGuard`](crate::exec::permission::ComputedFieldGuard)). One
/// copy serves every guard of the record, so the guards reading a field read
/// one derivation of it. The caller receives each computed field as the caller
/// derives it, and `$value` stays the value the caller would receive.
///
/// The view is derived out of line ([`out_of_line`](crate::exec::vm::out_of_line))
/// and answered boxed. Every caller awaits this once per record and holds the
/// view across its passes, and the VM's row driver awaits those callers in
/// place, so in an unoptimised build a derivation or a view held inline would
/// grow the driver's frame by its whole size.
async fn guard_view(
	ctx: &ExecutionContext,
	state: &FieldState,
	record: &Value,
) -> Result<Option<Box<Value>>, ControlFlow> {
	let Some(reads) = state.guard_reads.as_deref() else {
		return Ok(None);
	};
	if !matches!(record, Value::Object(_)) {
		return Ok(None);
	}
	crate::exec::vm::out_of_line(|| derive_guard_view(ctx, reads, record)).await
}

/// [`guard_view`]'s copy of `record`, an object, with the computed fields
/// `reads` reaches derived onto it, or `None` where they reach none.
async fn derive_guard_view(
	ctx: &ExecutionContext,
	reads: &GuardReads,
	record: &Value,
) -> Result<Option<Box<Value>>, ControlFlow> {
	let frame = ctx.with_perms_suppressed(true);
	let Some(derivation) = reads.state(&frame).await? else {
		return Ok(None);
	};
	let mut view = Box::new(record.clone());
	compute_fields_for_value(&frame, derivation, &mut view, true).await?;
	Ok(Some(view))
}

/// The body of [`filter_fields_by_permission`], restricted to one
/// [`PermissionPass`], with the two documents it reads separable, and reporting
/// whether any path was cut from `value`.
///
/// `snapshot` is the unmutated view that `Value::each` expansion and the
/// `$value` binding read, so that a cut made for one field cannot perturb the
/// paths or values a later field is judged on. It defaults to a lazy clone of
/// `value`; a caller that already holds an identical copy passes it to save the
/// clone. It must never be a view `value` has since diverged from — the paths
/// it yields are applied to `value`, and `$value` has to be the value the
/// caller would actually receive.
///
/// `predicate_doc` is the document a `Permission::Specific` clause is evaluated
/// against, defaulting to `snapshot`. A caller that has already reduced `value`
/// passes the unreduced record: the reduced view is missing the fields the
/// caller may not select, so a clause naming one of them reads it as `NONE`
/// there and passes a field the clause was written to deny — the same reason
/// the reduce kernel answers a stored field's clause against the record. Where
/// a clause reads a computed field, it is the guards' view of that record
/// ([`guard_view`]).
async fn filter_fields_by_permission_inner(
	ctx: &ExecutionContext,
	state: &FieldState,
	value: &mut Value,
	pass: PermissionPass,
	snapshot: Option<&Value>,
	predicate_doc: Option<&Value>,
	judged: &[usize],
) -> Result<bool, ControlFlow> {
	if state.field_permissions.is_empty() {
		return Ok(false);
	}
	if !matches!(value, Value::Object(_)) {
		return Ok(false);
	}

	// Snapshot the row only when we actually need to evaluate something
	// against the unmutated document, and only when the caller did not supply
	// one. Per-field denies cut from `value`; predicates and `each` read from
	// the snapshot so earlier cuts don't affect later field expansion.
	let mut lazy_snapshot: Option<Value> = None;
	// SECURITY: gather every concrete path to cut against the immutable snapshot
	// and apply them only once the loop has judged every field. `Value::cut`
	// removes an array element through `Vec::remove`, shifting the elements after
	// it down, so cutting as each field is judged would move a survivor under a
	// concrete index a later overlapping field had already derived from the same
	// snapshot — leaving that field's denial on the wrong element
	// (GHSA-wjh3-m4pv-q6cp). The old code reversed the indices within one field
	// for this reason; deferring extends that guarantee across every field that
	// shares an array.
	let mut to_cut: Vec<crate::expr::Idiom> = Vec::new();
	for (entry, (idiom, perm)) in state.field_permissions.iter().enumerate() {
		if !pass.applies_to(state, idiom) || judged.contains(&entry) {
			continue;
		}
		match perm {
			PhysicalPermission::Allow => continue,
			PhysicalPermission::Deny => {
				let original = match snapshot {
					Some(snapshot) => snapshot,
					None => &*lazy_snapshot.get_or_insert_with(|| value.clone()),
				};
				to_cut.extend(original.each(&idiom.0));
			}
			PhysicalPermission::Conditional(_) => {
				let original = match snapshot {
					Some(snapshot) => snapshot,
					None => &*lazy_snapshot.get_or_insert_with(|| value.clone()),
				};
				let doc = predicate_doc.unwrap_or(original);
				// Predicates read from `original` and `doc`, both immutable, so the
				// order the paths are judged in is irrelevant; the cut order is
				// settled by `apply_field_permission_cuts`.
				for path in original.each(&idiom.0) {
					let field_value = original.pick(&path.0);
					let allowed = check_permission_for_value(perm, doc, Some(&field_value), ctx)
						.await
						.map_err(|e| {
							ControlFlow::Err(anyhow::anyhow!(
								"Failed to check field permission: {e}"
							))
						})?;
					if !allowed {
						to_cut.push(path);
					}
				}
			}
		}
	}

	Ok(apply_field_permission_cuts(value, to_cut))
}

/// Cuts the denied concrete paths [`filter_fields_by_permission_inner`]
/// gathered, in the one order in which removing one never invalidates a path
/// still to be cut, and reports whether anything was cut.
///
/// Every path indexes into the single snapshot the whole pass was judged
/// against. A path ending at an array element removes that element and shifts
/// every later element of the array down, so the paths are cut greatest-first:
/// ordered by their index structure and applied in reverse, at each array the
/// higher element indices go before the lower, and an element's own fields
/// before the element itself — the one order `Value::cut`'s `Vec::remove` leaves
/// the remaining paths valid under. Identical paths are cut once, so an element
/// two fields both denied is not removed twice (the second removal would take
/// its neighbour).
fn apply_field_permission_cuts(value: &mut Value, mut paths: Vec<crate::expr::Idiom>) -> bool {
	if paths.is_empty() {
		return false;
	}
	// Order the paths so that reverse iteration cuts the greatest first: at each
	// array the higher element indices before the lower, and an element's fields
	// before the element itself. The comparator borrows the paths rather than
	// building a key per segment, and `dedup` drops the identical paths it has
	// made adjacent, so an element two fields both denied is not removed twice.
	paths.sort_by(|a, b| cut_path_order(&a.0, &b.0));
	paths.dedup();
	for path in paths.into_iter().rev() {
		value.cut(&path.0);
	}
	true
}

/// Orders two concrete cut paths by their index structure, so reverse iteration
/// of the sorted paths cuts the greatest first. Compared segment by segment: an
/// array element by its numeric index, an element before any other segment kind,
/// and a shorter path (a prefix of another) before the longer. The order only
/// has to be total: two paths that differ in kind at one position address
/// different shapes of the same value and never share an element whose index
/// could drift between them.
fn cut_path_order(
	a: &[crate::expr::part::Part],
	b: &[crate::expr::part::Part],
) -> std::cmp::Ordering {
	use std::cmp::Ordering;
	for (pa, pb) in a.iter().zip(b.iter()) {
		let ordering = match (pa.as_old_index(), pb.as_old_index()) {
			(Some(ia), Some(ib)) => ia.cmp(&ib),
			(Some(_), None) => Ordering::Less,
			(None, Some(_)) => Ordering::Greater,
			(None, None) => cut_segment_order(pa, pb),
		};
		if ordering != Ordering::Equal {
			return ordering;
		}
	}
	a.len().cmp(&b.len())
}

/// A total order over two non-index path segments. A field sorts by its name; any
/// other kind, which only a set element reaches here, by a stable rendering. The
/// order between two such segments does not affect the result: they address
/// different object fields, which never share an element.
fn cut_segment_order(
	a: &crate::expr::part::Part,
	b: &crate::expr::part::Part,
) -> std::cmp::Ordering {
	use crate::expr::part::Part;
	match (a, b) {
		(Part::Field(fa), Part::Field(fb)) => fa.as_str().cmp(fb.as_str()),
		_ => format!("{a:?}").cmp(&format!("{b:?}")),
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::exec::operators::test_util::{TestDb, field_idiom, parse_path, root_ctx};
	use crate::expr::logical::{LogicalOrder, LogicalOrdering};

	// =========================================================================
	// Fixture helpers
	// =========================================================================

	/// Assemble a [`FieldState`] directly, for the cases whose subject is the
	/// runtime behaviour of a given `(idiom, permission)` list or computed-field
	/// list rather than how the catalog resolves into one.
	fn state(
		computed_fields: Vec<ComputedFieldDef>,
		field_permissions: Vec<(crate::expr::Idiom, PhysicalPermission)>,
	) -> FieldState {
		FieldState {
			computed_fields,
			field_permissions: Arc::new(field_permissions),
			dep_map: Arc::new(HashMap::new()),
			permission_field_deps: Arc::new(HashSet::new()),
			permission_deps_complete: true,
			derivations: None,
			guard_reads: None,
		}
	}

	/// One `ORDER BY` key.
	fn order(field: &str, ascending: bool) -> LogicalOrder {
		LogicalOrder {
			value: parse_path(field),
			collate: false,
			numeric: false,
			direction: ascending,
		}
	}

	/// An `ORDER BY` clause over a single field.
	fn order_by(field: &str, ascending: bool) -> LogicalOrdering {
		LogicalOrdering::Order(vec![order(field, ascending)])
	}

	/// A value from its SurrealQL text, for the cut-ordering tests below.
	fn val(src: &str) -> Value {
		Value::from(crate::syn::value(src).expect("value should parse"))
	}

	// =========================================================================
	// ScanPipeline::compute_needs_processing / compute_needs_row_filtering
	// =========================================================================

	#[tokio::test]
	async fn an_unrestricted_scan_needs_neither_processing_nor_row_filtering() {
		let empty = FieldState::empty();
		assert!(!ScanPipeline::compute_needs_processing(
			&PhysicalPermission::Allow,
			&empty,
			true,
			None
		));
		assert!(!ScanPipeline::compute_needs_row_filtering(&PhysicalPermission::Allow, None));
	}

	#[tokio::test]
	async fn computed_fields_need_processing_but_are_not_row_filtering() {
		let with_computed = state(vec![ComputedFieldDef::for_test("total")], Vec::new());
		assert!(ScanPipeline::compute_needs_processing(
			&PhysicalPermission::Allow,
			&with_computed,
			false,
			None
		));
		// Computed fields rewrite rows in place and preserve both the row count
		// and their order, so positional pushdown stays sound.
		assert!(!ScanPipeline::compute_needs_row_filtering(&PhysicalPermission::Allow, None));
	}

	#[tokio::test]
	async fn field_permissions_need_processing_only_when_perms_are_checked() {
		let with_field_perms =
			state(Vec::new(), vec![(field_idiom("secret"), PhysicalPermission::Deny)]);

		assert!(ScanPipeline::compute_needs_processing(
			&PhysicalPermission::Allow,
			&with_field_perms,
			true,
			None
		));
		// With enforcement off the list is inert.
		assert!(!ScanPipeline::compute_needs_processing(
			&PhysicalPermission::Allow,
			&with_field_perms,
			false,
			None
		));
		// Cutting a field never drops a row.
		assert!(!ScanPipeline::compute_needs_row_filtering(&PhysicalPermission::Allow, None));
	}

	// =========================================================================
	// ScanPipeline::process_batch — limit/start state across batches
	// =========================================================================

	// =========================================================================
	// filter_and_process_batch — documented stage order
	// =========================================================================

	// =========================================================================
	// compute_fields_for_value
	// =========================================================================

	#[tokio::test]
	async fn computing_a_field_on_a_non_object_row_is_an_error() {
		let ctx = root_ctx();
		let field_state = state(vec![ComputedFieldDef::for_test("x")], Vec::new());

		let mut row = Value::from(1);
		let err = compute_fields_for_value(&ctx, &field_state, &mut row, false).await.unwrap_err();
		assert!(
			err.to_string().contains("Value is not an object"),
			"expected the non-object message, got {err}"
		);

		// With nothing to compute the same row is left alone.
		let mut row = Value::from(1);
		compute_fields_for_value(&ctx, &FieldState::empty(), &mut row, false).await.unwrap();
		assert_eq!(row, Value::from(1));
	}

	// =========================================================================
	// filter_fields_by_permission
	// =========================================================================

	/// The denied concrete paths two overlapping array permissions gather index
	/// the single pre-filter snapshot. Cutting one that removes an array element
	/// shifts the elements after it, so applying them must preserve each
	/// survivor's identity whatever order they were gathered in — a later denial
	/// must never land on a survivor that moved into a removed element's index
	/// (GHSA-wjh3-m4pv-q6cp).
	#[test]
	fn overlapping_array_cuts_preserve_element_identity() {
		let items = "[{ n: 0, secret: 's0' }, { n: 1, secret: 's1' }, \
		              { n: 2, secret: 's2' }, { n: 3, secret: 's3' }]";
		// Parent `items[*]` keeps even `n`, denying items[1] and items[3]; child
		// `items[*].secret` keeps only 's1', denying the secret of items[0],
		// items[2] and items[3]. Gathered parent-first, child-second, as the loop
		// walks the entries.
		let mut value = val(&format!("{{ items: {items} }}"));
		let gathered = vec![
			field_idiom("items[1]"),
			field_idiom("items[3]"),
			field_idiom("items[0].secret"),
			field_idiom("items[2].secret"),
			field_idiom("items[3].secret"),
		];
		assert!(apply_field_permission_cuts(&mut value, gathered));
		// n=2 survives the parent and shifts into index 1, but its denied 's2'
		// must still be gone — the drift left it visible.
		assert_eq!(value, val("{ items: [{ n: 0 }, { n: 2 }] }"));

		// The gather order must not change the outcome.
		let mut value = val(&format!("{{ items: {items} }}"));
		let gathered = vec![
			field_idiom("items[3].secret"),
			field_idiom("items[2].secret"),
			field_idiom("items[0].secret"),
			field_idiom("items[3]"),
			field_idiom("items[1]"),
		];
		assert!(apply_field_permission_cuts(&mut value, gathered));
		assert_eq!(value, val("{ items: [{ n: 0 }, { n: 2 }] }"));
	}

	/// An element two overlapping fields both deny is removed once: cutting the
	/// same index twice would take the element that shifted into it.
	#[test]
	fn a_doubly_denied_element_is_cut_once() {
		let mut value = val("{ items: [{ n: 0 }, { n: 1 }, { n: 2 }] }");
		let gathered =
			vec![field_idiom("items[1]"), field_idiom("items[1]"), field_idiom("items[2]")];
		assert!(apply_field_permission_cuts(&mut value, gathered));
		assert_eq!(value, val("{ items: [{ n: 0 }] }"));
	}

	/// Nested arrays with overlapping wildcard depths: an outer element removal
	/// and inner element removals judged against the same snapshot keep their
	/// identities, the inner cuts landing before the outer compaction shifts them.
	#[test]
	fn nested_array_cuts_preserve_identity() {
		let mut value = val("{ items: [\
			    { n: 0, tags: ['a', 'b'] }, \
			    { n: 1, tags: ['c', 'd'] }, \
			    { n: 2, tags: ['e', 'f'] }] }");
		// Outer denies the whole items[1]; inner denies the first tag of the
		// surviving elements by their original index.
		let gathered = vec![
			field_idiom("items[1]"),
			field_idiom("items[0].tags[0]"),
			field_idiom("items[2].tags[0]"),
		];
		assert!(apply_field_permission_cuts(&mut value, gathered));
		assert_eq!(value, val("{ items: [{ n: 0, tags: ['b'] }, { n: 2, tags: ['f'] }] }"));
	}

	/// With nothing gathered the value is untouched and no cut is reported.
	#[test]
	fn no_gathered_paths_cuts_nothing() {
		let mut value = val("{ items: [{ n: 0 }] }");
		assert!(!apply_field_permission_cuts(&mut value, Vec::new()));
		assert_eq!(value, val("{ items: [{ n: 0 }] }"));
	}

	// =========================================================================
	// filter_field_state_for_projection
	// =========================================================================

	// =========================================================================
	// build_field_state
	// =========================================================================

	#[tokio::test]
	async fn a_plain_table_resolves_to_the_empty_field_state() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE FIELD name ON t TYPE string;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let field_state = build_field_state(&ctx, &TableName::from("t"), true, None).await.unwrap();
		assert!(field_state.computed_fields.is_empty());
		assert!(field_state.field_permissions.is_empty());
	}

	#[tokio::test]
	async fn field_state_is_compiled_once_per_table_version_and_check_perms_flag() {
		use crate::exec::vm::descriptor::field_state;
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE FIELD score ON t TYPE int;
			 DEFINE FIELD doubled ON t TYPE int COMPUTED score * 2;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let table = TableName::from("t");

		let first = field_state(&ctx, &table, true).await.unwrap();
		let again = field_state(&ctx, &table, true).await.unwrap();
		assert!(Arc::ptr_eq(&first, &again), "the state is compiled once per table version");

		// The flag is part of the key: the two states differ, so they cannot
		// share an entry.
		let unchecked = field_state(&ctx, &table, false).await.unwrap();
		assert!(!Arc::ptr_eq(&first, &unchecked));
		assert!(Arc::ptr_eq(&unchecked, &field_state(&ctx, &table, false).await.unwrap()));
	}

	// =========================================================================
	// derivation relative to the select guard
	// =========================================================================

	/// A row of table `t` as storage holds it.
	fn stored(text: &str) -> Value {
		Value::from(crate::syn::value(text).expect("the row parses"))
	}

	#[tokio::test]
	async fn a_hidden_computed_field_is_never_derived_for_a_checked_caller() {
		// The body of `hidden` raises on this row, so deriving it would fail
		// the read.
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE FIELD shown ON t COMPUTED a + 1;
			 DEFINE FIELD hidden ON t COMPUTED <int> label PERMISSIONS FOR select NONE;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let table = TableName::from("t");
		let checked = build_field_state(&ctx, &table, true, None).await.unwrap();
		assert!(checked.deferred_derivations().is_some(), "the denied field is planned hidden");

		let mut row = stored("{ id: t:1, a: 1, label: 'x' }");
		materialise_fields_with_permissions(&ctx, &checked, &mut row, false, true)
			.await
			.expect("the hidden body is never evaluated");
		assert_eq!(row, stored("{ id: t:1, a: 1, label: 'x', shown: 2 }"));

		// A shadowing stored key of the hidden field goes with it.
		let mut row = stored("{ id: t:1, a: 1, label: 'x', hidden: 5 }");
		materialise_fields_with_permissions(&ctx, &checked, &mut row, false, true).await.unwrap();
		assert_eq!(row, stored("{ id: t:1, a: 1, label: 'x', shown: 2 }"));

		// A state that derives every field reaches the body and raises.
		let unchecked = build_field_state(&ctx, &table, false, None).await.unwrap();
		let mut row = stored("{ id: t:1, a: 1, label: 'x' }");
		assert!(compute_fields_for_value(&ctx, &unchecked, &mut row, false).await.is_err());
	}

	#[tokio::test]
	async fn a_judge_first_computed_field_is_derived_only_for_the_rows_its_guard_admits() {
		// The guard reads the stored `visible`; the body raises on a label that
		// is not a number, so a denied row proves the body did not run.
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE FIELD judged ON t COMPUTED <int> label PERMISSIONS FOR select WHERE visible = true;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let checked = build_field_state(&ctx, &TableName::from("t"), true, None).await.unwrap();
		assert!(checked.deferred_derivations().is_some(), "the guard is judged first");

		let mut admitted = stored("{ id: t:1, visible: true, label: '7' }");
		materialise_fields_with_permissions(&ctx, &checked, &mut admitted, false, true)
			.await
			.unwrap();
		assert_eq!(admitted, stored("{ id: t:1, visible: true, label: '7', judged: 7 }"));

		let mut denied = stored("{ id: t:2, visible: false, label: 'x' }");
		materialise_fields_with_permissions(&ctx, &checked, &mut denied, false, true)
			.await
			.expect("a denied row's body is never evaluated");
		assert_eq!(denied, stored("{ id: t:2, visible: false, label: 'x' }"));
	}

	#[tokio::test]
	async fn a_table_whose_derivations_are_all_as_main_carries_no_plan() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE FIELD doubled ON t COMPUTED a * 2;
			 DEFINE FIELD secret ON t PERMISSIONS FOR select NONE;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let checked = build_field_state(&ctx, &TableName::from("t"), true, None).await.unwrap();
		assert!(checked.deferred_derivations().is_none(), "`doubled` has no guard to defer to");

		let mut row = stored("{ id: t:1, a: 2, secret: 's' }");
		materialise_fields_with_permissions(&ctx, &checked, &mut row, false, true).await.unwrap();
		assert_eq!(row, stored("{ id: t:1, a: 2, doubled: 4 }"));
	}

	#[tokio::test]
	async fn narrowing_a_projection_keeps_the_plan_aligned_with_the_fields() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE FIELD shown ON t COMPUTED a + 1;
			 DEFINE FIELD hidden ON t COMPUTED a + 2 PERMISSIONS FOR select NONE;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let table = TableName::from("t");
		let wanted: HashSet<String> = ["hidden".to_owned()].into_iter().collect();
		let narrowed = build_field_state(&ctx, &table, true, Some(&wanted)).await.unwrap();
		assert_eq!(narrowed.computed_fields.len(), 1);
		let plan = narrowed.deferred_derivations().expect("the hidden field stays planned");
		assert_eq!(plan.len(), narrowed.computed_fields.len());
		assert_eq!(plan[0].plan, Derivation::Hidden);

		let mut row = stored("{ id: t:1, a: 1 }");
		materialise_fields_with_permissions(&ctx, &narrowed, &mut row, false, true).await.unwrap();
		assert_eq!(row, stored("{ id: t:1, a: 1 }"));
	}

	// =========================================================================
	// eval_limit_expr
	// =========================================================================

	// =========================================================================
	// determine_scan_direction
	// =========================================================================

	#[tokio::test]
	async fn ordering_by_id_descending_scans_backward() {
		let ordering = order_by("id", false);
		assert_eq!(determine_scan_direction(Some(&ordering)), Direction::Backward);
	}

	#[tokio::test]
	async fn every_other_ordering_scans_forward() {
		// Ascending `id` is the storage order already.
		let id_asc = order_by("id", true);
		assert_eq!(determine_scan_direction(Some(&id_asc)), Direction::Forward);

		// A descending sort on another field says nothing about key order.
		let name_desc = order_by("name", false);
		assert_eq!(determine_scan_direction(Some(&name_desc)), Direction::Forward);

		// Leading `name DESC` decides the direction; a later `id DESC` does not.
		let name_then_id = LogicalOrdering::Order(vec![order("name", false), order("id", false)]);
		assert_eq!(determine_scan_direction(Some(&name_then_id)), Direction::Forward);

		// No ORDER BY, an empty list, and ORDER BY RAND() all scan forward.
		assert_eq!(determine_scan_direction(None), Direction::Forward);
		let empty = LogicalOrdering::Order(Vec::new());
		assert_eq!(determine_scan_direction(Some(&empty)), Direction::Forward);
		assert_eq!(determine_scan_direction(Some(&LogicalOrdering::Random)), Direction::Forward);
	}

	// =========================================================================
	// decode_record
	// =========================================================================
}
