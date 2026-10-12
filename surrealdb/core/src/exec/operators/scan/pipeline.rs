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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use common::future::stream::{self, Yielder};

use crate::catalog::Permission;
use crate::catalog::providers::TableProvider;
use crate::exec::permission::{
	GuardKind, PhysicalPermission, check_permission_for_value, read_permission_to_physical,
};
use crate::exec::pre_decode_filter::{PreDecodeFilter, PreDecodeFilterOutcome};
use crate::exec::topk_pushdown::TopKThresholdProbe;
use crate::exec::{Clause, EvalContext, ExecutionContext, ValueBatch, ValueBatchStream};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::key::schema::RecordKey;
use crate::key::{KVKeyDecode, KVValue, RawRange};
use crate::kvs::{Direction, Transaction};
use crate::val::{TableName, Value};

/// A raw computed field entry before topological sorting:
/// `(field_name, clause, optional_kind, dependency_field_names)`.
type RawComputedField =
	(String, Clause, Option<crate::expr::Kind>, Vec<String>, Option<crate::iam::AuthLimit>);

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
/// happens inline, no intermediate owned `Vec<u8>` allocation per row.
#[allow(clippy::too_many_arguments)]
pub(crate) fn kv_scan_stream(
	txn: Arc<Transaction>,
	range: RawRange,
	version: Option<u64>,
	storage_limit: Option<usize>,
	direction: Direction,
	pre_skip: usize,
	limit_hint: Option<u32>,
	pre_decode_filter: Option<Arc<PreDecodeFilter>>,
	topk_probe: Option<Arc<TopKThresholdProbe>>,
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
			// Drive the cursor one borrowed row at a time. The visitor runs the
			// pre-decode filter and decodes survivors straight from the engine's
			// borrowed bytes; `decode_record` yields a fully owned `Value`, so
			// the raw bytes are never retained and the engine hands them over
			// with no per-row copy.
			let mut decoded: Vec<Value> = Vec::with_capacity(batch_size as usize);
			// `decode_record`'s error is a `ControlFlow`, which can't travel
			// through the visitor's storage-error channel — stash it and
			// re-raise it below, outside the cursor borrow.
			let mut decode_err: Option<ControlFlow> = None;
			let pdf = pre_decode_filter.as_ref();
			// Snapshot the TopK rejection threshold once per cursor batch.
			// The sort publishes monotonically-tightening values, so a stale
			// snapshot only under-rejects (bounded by one batch) — sound, and
			// it keeps the lock off the per-row path. `None` until the
			// downstream heap fills (or when no publisher was installed).
			let topk_threshold = topk_probe.as_ref().and_then(|p| p.snapshot());
			let mut topk_skipped: u64 = 0;
			let stats = cursor
				.for_each(batch_size, &mut |key, val| {
					if let Some(pdf) = pdf
						&& pdf.apply(key, val) == PreDecodeFilterOutcome::Reject
					{
						// Rejected rows are still counted as scanned (the cursor
						// read them), so `stats.rows` matches the old batch len.
						return Ok(std::ops::ControlFlow::Continue(()));
					}
					// After the (cheaper, more selective) WHERE probe: skip
					// decode when the row's ORDER BY key provably cannot beat
					// the downstream top-K heap's worst entry.
					if let (Some(probe), Some(threshold)) =
						(topk_probe.as_ref(), topk_threshold.as_deref())
						&& probe.rejects(threshold, val)
					{
						topk_skipped += 1;
						return Ok(std::ops::ControlFlow::Continue(()));
					}
					match decode_record(key, val) {
						Ok(v) => {
							decoded.push(v);
							Ok(std::ops::ControlFlow::Continue(()))
						}
						Err(cf) => {
							decode_err = Some(cf);
							Ok(std::ops::ControlFlow::Break(()))
						}
					}
				})
				.await
				.context("Failed to scan record")?;
			if topk_skipped > 0
				&& let Some(m) = topk_probe.as_ref().and_then(|p| p.metrics())
			{
				m.add_skipped_rows(topk_skipped);
			}
			// Re-raise a stashed decode error as the stream's terminal error,
			// now that the cursor borrow has ended. `?` on an `Err` already
			// ends the stream, so no explicit `return` is needed.
			if let Some(cf) = decode_err {
				Err(cf)?;
			}
			first = false;
			// `stats.rows` counts every row the cursor advanced over (including
			// pre-decode-filter rejects), matching the previous `batch.len()`.
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
				yielder.emit(ValueBatch::new(decoded)).await;
			}
		}
		Ok(())
	});

	Box::pin(stream)
}

/// Decode a record from its key and value bytes.
#[inline]
pub(crate) fn decode_record(key: &[u8], val: &[u8]) -> Result<Value, ControlFlow> {
	let decoded_key = RecordKey::decode_key(key).context("Failed to decode record key")?;

	let rid = crate::val::RecordId {
		table: decoded_key.tb.into_owned(),
		key: decoded_key.id.into_owned(),
	};

	let record = crate::catalog::Record::kv_decode_value(val, rid)
		.context("Failed to deserialize record")?;

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
	/// expression on this table. These root field names must be added to the
	/// projection-driven "needed" set before deciding which computed fields
	/// to evaluate — otherwise a `SELECT a` could skip computing field `b`
	/// while still applying a field permission whose expression references
	/// `b`, producing a permission decision against an incomplete row.
	/// `is_complete = false` (opaque expression) collapses into
	/// `permission_deps_complete = false`, which forces evaluation of all
	/// computed fields.
	permission_field_deps: Arc<HashSet<String>>,
	/// Whether `permission_field_deps` is exhaustive. False when any field
	/// permission expression contains opaque constructs (subqueries, params,
	/// etc.) that could reference fields outside of `permission_field_deps`.
	permission_deps_complete: bool,
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
		}
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
	/// (root Owner, which is also what pre-`auth_limit` field records default
	/// to), letting the common case skip the context clone.
	auth_limit: Option<crate::iam::AuthLimit>,
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
		}
	}
}

/// Convert a definition's stored `AUTH LIMIT` into the narrowing to apply to
/// its bodies, or `None` when the stamp cannot narrow any caller.
///
/// A root-`Owner` stamp is inert: `Level::Root` is a sublevel of nothing but
/// `Level::Root`, so the caller keeps its own level, and retaining roles `<=
/// Owner` keeps every role. Records written before `auth_limit` existed
/// default to exactly that stamp, so this is also the overwhelmingly common case.
pub(crate) fn narrowing_auth_limit(
	auth_limit: &crate::catalog::auth::AuthLimit,
) -> Result<Option<crate::iam::AuthLimit>, anyhow::Error> {
	if auth_limit == &crate::catalog::auth::AuthLimit::new_no_limit() {
		return Ok(None);
	}
	Ok(Some(crate::iam::AuthLimit::try_from(auth_limit)?))
}

/// Build field state from raw transaction and context parameters.
///
/// This is the core implementation that does the actual work: KV lookup of
/// field definitions, [`Clause`] compilation, dependency analysis, and
/// topological sorting. It takes explicit parameters instead of
/// `ExecutionContext`, making it usable at both plan time and execution time.
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
	// (runtime resolution), inner subqueries compile to runtime-resolving
	// scans. See
	// `language-tests/tests/reproductions/skip_fetch_perms_subquery_dereference.surql`.

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
			let clause = match body {
				Ok(body) => {
					let compiled =
						body_planner.scalar_metadata(body.clone()).await.with_context(|| {
							format!("Computed field '{field_name}' has unsupported expression")
						})?;
					Clause::of(body.clone(), compiled)
				}
				Err(reason) => Clause::uncompilable(reason.to_string()),
			};

			raw_computed.push((field_name, clause, fd.field_kind.clone(), deps.fields, limit));
		}
	}

	// Topologically sort ALL computed fields for correct evaluation order
	let topo_input: Vec<(String, Vec<String>)> =
		raw_computed.iter().map(|(name, _, _, deps, _)| (name.clone(), deps.clone())).collect();
	let sorted_indices = crate::expr::computed_deps::topological_sort_computed_fields(&topo_input);

	let mut computed_fields = Vec::with_capacity(sorted_indices.len());
	for idx in sorted_indices {
		let (field_name, expr, kind, _, auth_limit) = &raw_computed[idx];
		computed_fields.push(ComputedFieldDef {
			field_name: field_name.clone(),
			expr: expr.clone(),
			kind: kind.clone(),
			auth_limit: auth_limit.clone(),
		});
	}

	// Build field permissions, preserving each field's original Idiom so
	// `filter_fields_by_permission` can expand wildcards via `Value::each`.
	// `Permission::Full` entries are skipped — they're "always allow" and
	// don't need a runtime check.
	//
	// While walking conditional permissions, accumulate the set of fields
	// the expression references. `filter_field_state_for_projection` adds
	// these to the projection-driven "needed" set so that any computed
	// field referenced by a permission expression is evaluated even when
	// the user's SELECT didn't list it — otherwise a permission decision
	// would be made against an incomplete row.
	let mut field_permissions: Vec<(crate::expr::Idiom, PhysicalPermission)> = Vec::new();
	let mut permission_field_deps: HashSet<String> = HashSet::new();
	let mut permission_deps_complete = true;
	if check_perms {
		for fd in field_defs.iter() {
			let physical_perm = match &fd.select_permission {
				Permission::Full => continue,
				Permission::None => PhysicalPermission::Deny,
				Permission::Specific(expr) => {
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

	Ok(FieldState {
		computed_fields,
		field_permissions: Arc::new(field_permissions),
		dep_map: Arc::new(dep_map),
		permission_field_deps: Arc::new(permission_field_deps),
		permission_deps_complete,
	})
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
/// SECURITY: even when the projection is selective, computed fields
/// referenced by any conditional `PERMISSIONS FOR select WHERE …`
/// expression on this table are always evaluated. Otherwise a
/// `SELECT a` could skip computing `b` while still applying a permission
/// on field `c` whose expression references `b`, producing a permission
/// decision against an incomplete row. If a permission expression had
/// opaque dependencies (subqueries, params), all computed fields are
/// evaluated.
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

	let computed_fields = if let Some(ref required_set) = required {
		full_state
			.computed_fields
			.iter()
			.filter(|cf| required_set.contains(&cf.field_name))
			.cloned()
			.collect()
	} else {
		full_state.computed_fields.clone()
	};

	FieldState {
		computed_fields,
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
/// A key written while a field was ordinary stays in the record's bytes once
/// that field is redefined as COMPUTED. The field no longer reaches it, but a
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
	let shadowed = match value {
		Value::Object(obj) => state.dep_map.keys().any(|name| obj.contains_key(name.as_str())),
		_ => false,
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
	if state.computed_fields.is_empty() {
		return Ok(());
	}

	let mut eval_ctx = EvalContext::from_exec_ctx(ctx);
	eval_ctx.skip_fetch_perms = skip_fetch_perms;

	// Extract the record ID before entering the loop so that field
	// dereferences that target this same record are answered directly
	// instead of re-computing fields (which would loop forever).
	eval_ctx.computing_record = match &*value {
		Value::Object(obj) => match obj.get("id") {
			Some(Value::RecordId(rid)) => {
				Some(Arc::new(crate::exec::eval_context::ComputingRecord {
					rid: rid.clone(),
					stored: stored_view,
				}))
			}
			_ => None,
		},
		_ => None,
	};

	// SECURITY: the body is definer code, so it runs in a privileged frame that
	// holds none of the caller's bindings ([`ExecutionContext::privileged_frame`]),
	// and the frame refuses writes, whichever auth it holds.
	// `Expr::contains_mutation` rejects a mutation written into the body at
	// definition time but cannot see through a call to a user-defined function,
	// so without the refusal a read of the field would mutate the database as
	// the definer. Loop-invariant, so derived once per row rather than per field.
	let field_ctx = ctx.clone().privileged_frame().computing_field();

	for cf in &state.computed_fields {
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
		// nested subqueries see the same `$parent` as top-level projections (#7154).
		let row_ctx = narrowed_eval_ctx.with_value_and_doc(value);
		let computed_value = match cf.expr.evaluate(row_ctx).await {
			Ok(v) => v,
			Err(ControlFlow::Return(v)) => v,
			Err(e) => return Err(e),
		};

		// A value that does not coerce fails naming the computed field.
		let final_value = if let Some(kind) = &cf.kind {
			computed_value.coerce_to_kind(kind).map_err(|error| {
				ControlFlow::Err(
					anyhow::Error::new(error)
						.context(format!("Failed to coerce computed field '{}'", cf.field_name)),
				)
			})?
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
			// A computed field is a top-level one, so no longer path is its own
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
/// Computed fields are evaluated first, because a field-level permission
/// predicate may read one and must see the value the record computes to. That
/// leaves every computed field derived from the document *before* any field was
/// cut, so one whose body reads a denied field still holds its value once the
/// permission pass has removed the source. Re-deriving them against the reduced
/// document is what keeps the record consistent.
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
	// The computed values and their snapshot are both live now, and the passes
	// below may cut them before the batch's own threshold check would see them.
	super::common::ensure_below_memory_threshold()?;
	// Stored fields first. `value` still equals `record`, so it doubles as the
	// snapshot that `each` expansion and the `$value` binding read.
	let cut = filter_fields_by_permission_inner(
		ctx,
		state,
		value,
		PermissionPass::Stored,
		Some(&record),
		Some(&record),
	)
	.await?;
	if cut {
		// Re-derive the computed fields, now against the reduced record, so one
		// whose body reads a denied field yields `NONE` instead of the value it
		// held before that field was cut. The same reduced row backs the
		// self-dereference, so `id.<field>` reads neither the cut value nor a
		// shadowed stored one back out of storage.
		let stored_view = stored_fields_view(state, value);
		compute_fields_for_value_against(ctx, state, value, skip_fetch_perms, Some(stored_view))
			.await?;
		// The re-derived values are live now, and the computed fields' own
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
		Some(&record),
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
	filter_fields_by_permission_inner(ctx, state, value, PermissionPass::All, None, None).await?;
	Ok(())
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
/// the reduce kernel answers a stored field's clause against the record.
async fn filter_fields_by_permission_inner(
	ctx: &ExecutionContext,
	state: &FieldState,
	value: &mut Value,
	pass: PermissionPass,
	snapshot: Option<&Value>,
	predicate_doc: Option<&Value>,
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
	let mut cut_any = false;
	for (idiom, perm) in state.field_permissions.iter() {
		if !pass.applies_to(state, idiom) {
			continue;
		}
		match perm {
			PhysicalPermission::Allow => continue,
			PhysicalPermission::Deny => {
				let original = match snapshot {
					Some(snapshot) => snapshot,
					None => &*lazy_snapshot.get_or_insert_with(|| value.clone()),
				};
				// SECURITY: iterate in reverse. `each` yields ascending
				// array indices and `Value::cut` removes via `Vec::remove`
				// (shifting later indices down), so a forward pass would
				// let each removal invalidate the pending indices and leak
				// the odd-indexed elements (issue #7356). Removing higher
				// indices first keeps the pending lower indices valid.
				for path in original.each(&idiom.0).into_iter().rev() {
					value.cut(&path.0);
					cut_any = true;
				}
			}
			PhysicalPermission::Conditional(_) => {
				let original = match snapshot {
					Some(snapshot) => snapshot,
					None => &*lazy_snapshot.get_or_insert_with(|| value.clone()),
				};
				let doc = predicate_doc.unwrap_or(original);
				// SECURITY: iterate in reverse (see the Deny arm above and
				// issue #7356). Predicates read from `original` and `doc`,
				// both immutable, so evaluation order is irrelevant.
				for path in original.each(&idiom.0).into_iter().rev() {
					let field_value = original.pick(&path.0);
					let allowed = check_permission_for_value(perm, doc, Some(&field_value), ctx)
						.await
						.map_err(|e| {
							ControlFlow::Err(anyhow::anyhow!(
								"Failed to check field permission: {e}"
							))
						})?;
					if !allowed {
						value.cut(&path.0);
						cut_any = true;
					}
				}
			}
		}
	}

	Ok(cut_any)
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
