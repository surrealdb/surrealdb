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
	PhysicalPermission, check_permission_for_value, convert_permission_to_physical,
};
use crate::exec::pre_decode_filter::{PreDecodeFilter, PreDecodeFilterOutcome};
use crate::exec::topk_pushdown::TopKThresholdProbe;
use crate::exec::{EvalContext, ExecutionContext, PhysicalExpr, ValueBatch, ValueBatchStream};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::key::schema::RecordKey;
use crate::key::{KVKeyDecode, KVValue, RawRange};
use crate::kvs::{Direction, Transaction};
use crate::val::{TableName, Value};

/// A raw computed field entry before topological sorting:
/// `(field_name, physical_expr, optional_kind, dependency_field_names)`.
type RawComputedField = (
	String,
	Arc<dyn PhysicalExpr>,
	Option<crate::expr::Kind>,
	Vec<String>,
	Option<crate::iam::AuthLimit>,
);

// =============================================================================
// ScanPipeline
// =============================================================================

/// Inline pipeline that performs all per-batch operations (filtering, computed
/// fields, permissions, limit/start) in a single pass with minimal await
/// boundaries. Limit/start state is tracked across batches so the logic is
/// written once rather than duplicated in every scan path.
pub(crate) struct ScanPipeline {
	permission: PhysicalPermission,
	predicate: Option<Arc<dyn PhysicalExpr>>,
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
		predicate: Option<&Arc<dyn PhysicalExpr>>,
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
		predicate: Option<&Arc<dyn PhysicalExpr>>,
	) -> bool {
		!matches!(permission, PhysicalPermission::Allow) || predicate.is_some()
	}

	pub(crate) fn new(
		permission: PhysicalPermission,
		predicate: Option<Arc<dyn PhysicalExpr>>,
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
		}
	}

	/// Returns true when limit or start tracking is active.
	fn has_limit(&self) -> bool {
		self.limit.is_some() || self.start > 0
	}

	/// Process a single batch in-place: filter, compute fields, apply
	/// permissions, then apply limit/start. Returns `false` when the
	/// limit has been reached and the caller should stop iterating.
	pub(crate) async fn process_batch(
		&mut self,
		batch: &mut Vec<Value>,
		ctx: &ExecutionContext,
	) -> Result<bool, ControlFlow> {
		// Phase 1: filter + process (parallel per-record via try_join_all_buffered)
		if self.needs_processing {
			filter_and_process_batch(
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
pub(crate) fn determine_scan_direction(order: Option<&crate::expr::order::Ordering>) -> Direction {
	use crate::expr::order::Ordering as OrderingType;
	if let Some(OrderingType::Order(order_list)) = order
		&& let Some(first) = order_list.0.first()
		&& !first.direction
		&& first.value.is_id()
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
			if !decoded.is_empty() {
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
					// Bind the record as both current value and document root,
					// matching `check_permission_for_value` — see the note there
					// on why `$parent` needs the root.
					let mut eval_ctx = EvalContext::from_exec_ctx($ctx).with_value_and_doc($value);
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
/// Per-record pipeline (sequential, in-place):
///   table permission -> computed fields -> field permissions -> WHERE predicate.
/// Field-level permissions precede the predicate so a field the reader may not
/// see is already cut from the document the condition is evaluated against.
/// Records that fail any check are compacted out via an in-place swap so the
/// surviving prefix can be truncated at the end with no extra allocation.
pub(crate) async fn filter_and_process_batch(
	batch: &mut Vec<Value>,
	permission: &PhysicalPermission,
	predicate: Option<&Arc<dyn PhysicalExpr>>,
	ctx: &ExecutionContext,
	state: &FieldState,
	check_perms: bool,
) -> Result<(), ControlFlow> {
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
		return Ok(());
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
		// Computed fields (must run before predicate)
		compute_fields_for_value(ctx, state, &mut batch[write_idx], false).await?;
		// Field-level permissions (must run before the WHERE predicate so that
		// restricted fields are removed before the condition is evaluated,
		// matching the old compute path's behaviour).
		if check_perms {
			filter_fields_by_permission(ctx, state, &mut batch[write_idx]).await?;
		}
		// WHERE predicate (evaluated on the permission-reduced document)
		if let Some(pred) = predicate {
			let eval_ctx = EvalContext::from_exec_ctx(ctx).with_value_and_doc(&batch[write_idx]);
			if !pred.evaluate(eval_ctx).await?.is_truthy() {
				continue;
			}
		}
		write_idx += 1;
	}
	batch.truncate(write_idx);
	Ok(())
}

// =============================================================================
// Key helpers
// =============================================================================

/// Evaluate a limit or start expression to a usize value.
pub(crate) async fn eval_limit_expr(
	expr: &dyn PhysicalExpr,
	ctx: &ExecutionContext,
) -> Result<usize, ControlFlow> {
	let eval_ctx = EvalContext::from_exec_ctx(ctx);
	let value = expr
		.evaluate(eval_ctx)
		.await
		.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to evaluate LIMIT/START: {e}")))?;
	match &value {
		Value::Number(n) => {
			let i = (*n).to_int();
			if i >= 0 {
				Ok(i as usize)
			} else {
				Err(ControlFlow::Err(anyhow::anyhow!(
					"LIMIT/START must be a non-negative integer, got {i}"
				)))
			}
		}
		Value::None | Value::Null => Ok(0),
		_ => Err(ControlFlow::Err(anyhow::anyhow!(
			"LIMIT/START must be an integer, got {:?}",
			value
		))),
	}
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
	expr: Arc<dyn PhysicalExpr>,
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
	/// Root field name this computed-field definition is attached to.
	pub(crate) fn field_name(&self) -> &str {
		&self.field_name
	}

	/// Test-only constructor: production definitions are built exclusively by
	/// [`build_field_state_raw`] from catalog field definitions.
	#[cfg(test)]
	pub(crate) fn for_test(field_name: impl Into<String>) -> Self {
		Self {
			field_name: field_name.into(),
			expr: Arc::new(crate::exec::physical_expr::Literal(Value::None)),
			kind: None,
			auth_limit: None,
		}
	}
}

/// Convert a field's stored `AUTH LIMIT` into the narrowing to apply to its
/// bodies, or `None` when the stamp cannot narrow any caller.
///
/// A root-`Owner` stamp is inert: `Level::Root` is a sublevel of nothing but
/// `Level::Root`, so the caller keeps its own level, and retaining roles `<=
/// Owner` keeps every role. Field records written before `auth_limit` existed
/// default to exactly that stamp, so this is also the overwhelmingly common case.
fn narrowing_auth_limit(
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
/// field definitions, PhysicalExpr compilation, dependency analysis, and
/// topological sorting. It takes explicit parameters instead of
/// `ExecutionContext`, making it usable at both plan time and execution time.
pub(crate) async fn build_field_state_raw(
	planner: &crate::exec::planner::Planner<'_>,
	ns_id: crate::catalog::NamespaceId,
	db_id: crate::catalog::DatabaseId,
	table_name: &TableName,
	check_perms: bool,
	version: Option<u64>,
) -> Result<FieldState, ControlFlow> {
	let txn =
		planner.txn().context("build_field_state_raw requires a planner with a transaction")?;
	let field_defs = txn
		.all_tb_fields(ns_id, db_id, table_name, version)
		.await
		.context("Failed to get field definitions")?;

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
	// (runtime fallback), inner subqueries compile to runtime-resolving
	// scans — bit-for-bit identical to the legacy behaviour. See
	// `language-tests/tests/reproductions/skip_fetch_perms_subquery_dereference.surql`.

	// Collect ALL computed fields and their dependency metadata.
	let mut raw_computed: Vec<RawComputedField> = Vec::new();
	let mut dep_map: HashMap<String, crate::expr::computed_deps::ComputedDeps> = HashMap::new();

	for fd in field_defs.iter() {
		if let Some(ref expr) = fd.computed {
			let field_name = fd.name.to_raw_string();

			let deps = crate::expr::computed_deps::extract_computed_deps(expr);

			dep_map.insert(field_name.clone(), deps.clone());

			let physical_expr = planner.physical_expr(expr.clone()).await.with_context(|| {
				format!("Computed field '{field_name}' has unsupported expression")
			})?;

			raw_computed.push((
				field_name,
				physical_expr,
				fd.field_kind.clone(),
				deps.fields,
				narrowing_auth_limit(&fd.auth_limit)?,
			));
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
			expr: Arc::clone(expr),
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
			if matches!(fd.select_permission, Permission::Full) {
				continue;
			}
			if let Permission::Specific(ref expr) = fd.select_permission {
				let deps = crate::expr::computed_deps::extract_computed_deps(expr);
				if !deps.is_complete {
					// Read the flag *before* flipping it below so we only emit
					// on the first opaque field — one log line per table
					// build. `FieldState` is cached per `(table, check_perms)`,
					// so this fires once per distinct table per cache lifetime.
					if permission_deps_complete {
						crate::expr::computed_deps::warn_incomplete_perm_deps(
							table_name.as_str(),
							fd.name.to_raw_string().as_str(),
						);
					}
					permission_deps_complete = false;
				}
				permission_field_deps.extend(deps.fields);
			}
			let physical_perm = convert_permission_to_physical(&fd.select_permission, planner)
				.await
				.context("Failed to convert field permission")?;
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

/// Fetch field definitions and build the cached field state.
///
/// Always builds and caches the *full* field state (all computed fields and
/// permissions) keyed by `(table, check_perms)`. When `needed_fields` is
/// `Some`, the cached full state is cheaply filtered to the required subset.
/// This avoids repeated expensive work (KV lookups, PhysicalExpr compilation,
/// dependency analysis, topological sort) for projected queries.
pub(crate) async fn build_field_state(
	ctx: &ExecutionContext,
	table_name: &TableName,
	check_perms: bool,
	needed_fields: Option<&std::collections::HashSet<String>>,
) -> Result<FieldState, ControlFlow> {
	let db_ctx = ctx.database().context("build_field_state requires database context")?;
	let version = ctx.version_stamp();
	let cache_key = (table_name.clone(), check_perms);

	// Check the cache first (keyed by table name + check_perms flag).
	// Versioned reads bypass the cache to get field defs at the correct point in time.
	if version.is_none() {
		let cache = db_ctx.field_state_cache.read().await;
		if let Some(cached) = cache.get(&cache_key) {
			return Ok(filter_field_state_for_projection(cached, needed_fields));
		}
	}

	// Fresh `Planner::with_txn` so subqueries inside computed-field /
	// field-permission bodies get plan-time index resolution. The cycle
	// guard starts empty; same-table recursion is broken by the inner
	// `try_resolve_table_ctx` push.
	let planner = crate::exec::planner::Planner::for_database(ctx.ctx(), ctx.txn(), db_ctx);
	let full_state = build_field_state_raw(
		&planner,
		db_ctx.ns_ctx.ns.namespace_id,
		db_ctx.db.database_id,
		table_name,
		check_perms,
		version,
	)
	.await?;

	// Cache the full (unfiltered) state (skip for versioned reads)
	let cached = Arc::new(full_state);
	if version.is_none() {
		db_ctx.field_state_cache.write().await.insert(cache_key, Arc::clone(&cached));
	}

	// Return filtered if needed_fields is specified
	Ok(filter_field_state_for_projection(&cached, needed_fields))
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
	if state.computed_fields.is_empty() {
		return Ok(());
	}

	let mut eval_ctx = EvalContext::from_exec_ctx(ctx);
	eval_ctx.skip_fetch_perms = skip_fetch_perms;

	// Extract the record ID before entering the loop so that field
	// dereferences that target this same record can return raw data
	// instead of re-computing fields (which would loop forever).
	eval_ctx.computing_record = match &*value {
		Value::Object(obj) => match obj.get("id") {
			Some(Value::RecordId(rid)) => Some(rid.clone()),
			_ => None,
		},
		_ => None,
	};

	for cf in &state.computed_fields {
		// SECURITY: apply the field's AUTH LIMIT so the body runs under the
		// definer's auth, not the reader's. Without it a low-privileged definer
		// can plant a body that a high-privileged reader then executes with
		// their own privilege. Mirrors `Document::computed_fields_inner`.
		//
		// The narrowed context is derived per field rather than hoisted because
		// each field carries its own stamp; `narrowing_auth_limit` returns `None`
		// for the inert root-Owner stamp so the common case pays nothing.
		let limited_ctx = cf.auth_limit.as_ref().map(|limit| ctx.with_limited_auth(limit));
		let narrowed_eval_ctx = limited_ctx.as_ref().map(|limited| {
			let mut narrowed = EvalContext::from_exec_ctx(limited);
			narrowed.skip_fetch_perms = skip_fetch_perms;
			narrowed.computing_record.clone_from(&eval_ctx.computing_record);
			narrowed
		});
		// Evaluate with the row as both current value and document root so
		// nested subqueries see the same `$parent` as top-level projections (#7154).
		let row_ctx = narrowed_eval_ctx.as_ref().unwrap_or(&eval_ctx).with_value_and_doc(value);
		let computed_value = match cf.expr.evaluate(row_ctx).await {
			Ok(v) => v,
			Err(ControlFlow::Return(v)) => v,
			Err(e) => return Err(e),
		};

		// Apply type coercion if specified
		let final_value = if let Some(kind) = &cf.kind {
			computed_value
				.coerce_to_kind(kind)
				.with_context(|| format!("Failed to coerce computed field '{}'", cf.field_name))?
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

/// Filter fields from a value based on field-level permissions.
///
/// Each `(idiom, perm)` entry is expanded via [`Value::each`] to handle
/// wildcards (`outer.*`, `items[*]`), then each concrete path is checked
/// with `$value` bound to the picked field value — matching the legacy
/// [`crate::doc::pluck::Document::pluck_select`] semantics that the
/// streaming runtime previously skipped for nested paths (issue #83).
pub(crate) async fn filter_fields_by_permission(
	ctx: &ExecutionContext,
	state: &FieldState,
	value: &mut Value,
) -> Result<(), ControlFlow> {
	if state.field_permissions.is_empty() {
		return Ok(());
	}
	if !matches!(value, Value::Object(_)) {
		return Ok(());
	}

	// Snapshot the row only when we actually need to evaluate something
	// against the unmutated document. Per-field denies cut from `value`;
	// predicates and `each` read from the snapshot so earlier cuts don't
	// affect later field expansion.
	let mut snapshot: Option<Value> = None;
	for (idiom, perm) in state.field_permissions.iter() {
		match perm {
			PhysicalPermission::Allow => continue,
			PhysicalPermission::Deny => {
				let original = snapshot.get_or_insert_with(|| value.clone());
				// SECURITY: iterate in reverse. `each` yields ascending
				// array indices and `Value::cut` removes via `Vec::remove`
				// (shifting later indices down), so a forward pass would
				// let each removal invalidate the pending indices and leak
				// the odd-indexed elements (issue #7356). Removing higher
				// indices first keeps the pending lower indices valid.
				for path in original.each(&idiom.0).into_iter().rev() {
					value.cut(&path.0);
				}
			}
			PhysicalPermission::Conditional(_) => {
				let original = snapshot.get_or_insert_with(|| value.clone());
				// SECURITY: iterate in reverse (see the Deny arm above and
				// issue #7356). Predicates read from `original` (immutable),
				// so evaluation order is irrelevant.
				for path in original.each(&idiom.0).into_iter().rev() {
					let field_value = original.pick(&path.0);
					let allowed =
						check_permission_for_value(perm, original, Some(&field_value), ctx)
							.await
							.map_err(|e| {
							ControlFlow::Err(anyhow::anyhow!(
								"Failed to check field permission: {e}"
							))
						})?;
					if !allowed {
						value.cut(&path.0);
					}
				}
			}
		}
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use std::borrow::Cow;

	use super::*;
	use crate::exec::operators::test_util::{TestDb, parse_idiom, physical_expr, root_ctx, val};
	use crate::expr::computed_deps::ComputedDeps;
	use crate::expr::order::{Order, OrderList, Ordering};
	use crate::key::schema::RecordPrefix;
	use crate::val::Number;

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

	/// A computed-field definition with an explicit body, mirroring what
	/// `build_field_state_raw` produces for `DEFINE FIELD … COMPUTED …`.
	fn computed(
		field_name: &str,
		expr: Arc<dyn PhysicalExpr>,
		kind: Option<crate::expr::Kind>,
	) -> ComputedFieldDef {
		ComputedFieldDef {
			field_name: field_name.to_owned(),
			expr,
			kind,
			auth_limit: None,
		}
	}

	/// Build a batch from SurrealQL object literals.
	async fn rows(srcs: &[&str]) -> Vec<Value> {
		let mut out = Vec::with_capacity(srcs.len());
		for src in srcs {
			out.push(val(src).await);
		}
		out
	}

	/// Read a field path out of a row.
	fn pick(value: &Value, path: &str) -> Value {
		value.pick(&parse_idiom(path).0)
	}

	/// The `n` field of every row in a batch, for order-sensitive assertions.
	fn ns(batch: &[Value]) -> Vec<Value> {
		batch.iter().map(|v| pick(v, "n")).collect()
	}

	/// An `ORDER BY` clause over a single field.
	fn order_by(field: &str, ascending: bool) -> Ordering {
		Ordering::Order(OrderList(vec![Order {
			value: parse_idiom(field),
			direction: ascending,
			..Default::default()
		}]))
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
	async fn a_table_permission_needs_processing_and_removes_rows() {
		let ctx = root_ctx();
		let empty = FieldState::empty();

		for permission in [
			PhysicalPermission::Deny,
			PhysicalPermission::Conditional(physical_expr("public = true", &ctx).await),
		] {
			assert!(ScanPipeline::compute_needs_processing(&permission, &empty, true, None));
			// A table permission decides which rows survive, so positional
			// START/LIMIT pushdown must be suppressed.
			assert!(ScanPipeline::compute_needs_row_filtering(&permission, None));
		}
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
			state(Vec::new(), vec![(parse_idiom("secret"), PhysicalPermission::Deny)]);

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

	#[tokio::test]
	async fn a_predicate_needs_processing_and_removes_rows() {
		let ctx = root_ctx();
		let predicate = physical_expr("n > 2", &ctx).await;
		let empty = FieldState::empty();

		assert!(ScanPipeline::compute_needs_processing(
			&PhysicalPermission::Allow,
			&empty,
			false,
			Some(&predicate)
		));
		assert!(ScanPipeline::compute_needs_row_filtering(
			&PhysicalPermission::Allow,
			Some(&predicate)
		));
	}

	// =========================================================================
	// ScanPipeline::process_batch — limit/start state across batches
	// =========================================================================

	/// A pipeline with no processing work, so `process_batch` exercises only the
	/// limit/start bookkeeping.
	fn limit_pipeline(limit: Option<usize>, start: usize) -> ScanPipeline {
		ScanPipeline::new(PhysicalPermission::Allow, None, FieldState::empty(), false, limit, start)
	}

	/// A batch of `{ n: … }` rows numbered from `from`.
	async fn numbered(from: usize, count: usize) -> Vec<Value> {
		let srcs: Vec<String> = (from..from + count).map(|n| format!("{{ n: {n} }}")).collect();
		rows(&srcs.iter().map(String::as_str).collect::<Vec<_>>()).await
	}

	#[tokio::test]
	async fn a_batch_wholly_inside_the_start_offset_is_discarded_and_iteration_continues() {
		let ctx = root_ctx();
		let mut pipeline = limit_pipeline(None, 5);

		let mut batch = numbered(0, 4).await;
		assert!(pipeline.process_batch(&mut batch, &ctx).await.unwrap());
		assert!(batch.is_empty());
		assert_eq!(pipeline.skipped, 4);
		assert_eq!(pipeline.emitted, 0);
	}

	#[tokio::test]
	async fn the_start_offset_is_consumed_across_batches_and_skips_only_its_prefix() {
		let ctx = root_ctx();
		let mut pipeline = limit_pipeline(None, 5);

		let mut first = numbered(0, 4).await;
		assert!(pipeline.process_batch(&mut first, &ctx).await.unwrap());
		assert!(first.is_empty());

		// One row of the offset is left, so the second batch loses exactly its
		// first row and keeps the rest in order.
		let mut second = numbered(4, 4).await;
		assert!(pipeline.process_batch(&mut second, &ctx).await.unwrap());
		assert_eq!(ns(&second), vec![Value::from(5), Value::from(6), Value::from(7)]);
		assert_eq!(pipeline.skipped, 5);
		assert_eq!(pipeline.emitted, 3);
	}

	#[tokio::test]
	async fn the_batch_is_truncated_at_the_limit_and_iteration_stops_there() {
		let ctx = root_ctx();
		let mut pipeline = limit_pipeline(Some(2), 0);

		let mut batch = numbered(0, 5).await;
		// The limit is reached inside this batch, so the caller is told to stop.
		assert!(!pipeline.process_batch(&mut batch, &ctx).await.unwrap());
		assert_eq!(ns(&batch), vec![Value::from(0), Value::from(1)]);
		assert_eq!(pipeline.emitted, 2);
	}

	#[tokio::test]
	async fn the_limit_is_tracked_across_batches_and_only_goes_false_once_reached() {
		let ctx = root_ctx();
		let mut pipeline = limit_pipeline(Some(3), 0);

		let mut first = numbered(0, 2).await;
		// Two of three emitted: keep going.
		assert!(pipeline.process_batch(&mut first, &ctx).await.unwrap());
		assert_eq!(first.len(), 2);

		let mut second = numbered(2, 2).await;
		assert!(!pipeline.process_batch(&mut second, &ctx).await.unwrap());
		assert_eq!(ns(&second), vec![Value::from(2)]);
		assert_eq!(pipeline.emitted, 3);
	}

	#[tokio::test]
	async fn a_zero_limit_emits_nothing_and_stops_immediately() {
		let ctx = root_ctx();
		let mut pipeline = limit_pipeline(Some(0), 0);

		let mut batch = numbered(0, 3).await;
		assert!(!pipeline.process_batch(&mut batch, &ctx).await.unwrap());
		assert!(batch.is_empty());
	}

	#[tokio::test]
	async fn without_limit_or_start_the_batch_passes_through_untouched() {
		let ctx = root_ctx();
		let mut pipeline = limit_pipeline(None, 0);

		let mut batch = numbered(0, 3).await;
		assert!(pipeline.process_batch(&mut batch, &ctx).await.unwrap());
		assert_eq!(ns(&batch), vec![Value::from(0), Value::from(1), Value::from(2)]);
		// Nothing is counted when there is no limit or start to track.
		assert_eq!(pipeline.emitted, 0);
	}

	// =========================================================================
	// filter_and_process_batch — documented stage order
	// =========================================================================

	#[tokio::test]
	async fn a_row_denied_by_the_table_permission_never_reaches_the_predicate() {
		let ctx = root_ctx();
		let empty = FieldState::empty();
		// A predicate that cannot be evaluated without failing the whole batch.
		let poison = physical_expr("THROW 'the predicate must not see this row'", &ctx).await;

		// Unconditional deny: every row is dropped before the predicate runs.
		let mut batch = rows(&["{ n: 1 }", "{ n: 2 }"]).await;
		filter_and_process_batch(
			&mut batch,
			&PhysicalPermission::Deny,
			Some(&poison),
			&ctx,
			&empty,
			false,
		)
		.await
		.expect("no row reaches the predicate, so it cannot fail");
		assert!(batch.is_empty());

		// Conditional deny: only the permitted row is fed to the predicate, and
		// the predicate throws for anything else.
		let permission = PhysicalPermission::Conditional(physical_expr("public", &ctx).await);
		let guarded = physical_expr(
			"IF public { true } ELSE { THROW 'the predicate saw a denied row' }",
			&ctx,
		)
		.await;
		let mut batch =
			rows(&["{ n: 1, public: false }", "{ n: 2, public: true }", "{ n: 3, public: false }"])
				.await;
		filter_and_process_batch(&mut batch, &permission, Some(&guarded), &ctx, &empty, false)
			.await
			.expect("denied rows are cut before the predicate");
		assert_eq!(ns(&batch), vec![Value::from(2)]);
	}

	#[tokio::test]
	async fn a_computed_field_is_visible_to_the_where_predicate() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE FIELD score ON t TYPE int;
			 DEFINE FIELD doubled ON t TYPE int COMPUTED score * 2;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let field_state =
			build_field_state(&ctx, &TableName::from("t"), false, None).await.unwrap();
		assert_eq!(field_state.computed_fields.len(), 1);

		let predicate = physical_expr("doubled > 4", &ctx).await;
		let mut batch = rows(&["{ id: t:1, score: 3 }", "{ id: t:2, score: 1 }"]).await;
		filter_and_process_batch(
			&mut batch,
			&PhysicalPermission::Allow,
			Some(&predicate),
			&ctx,
			&field_state,
			false,
		)
		.await
		.unwrap();

		// The predicate could only decide this because the computed field was
		// injected before it ran.
		assert_eq!(batch.len(), 1);
		assert_eq!(pick(&batch[0], "id"), val("t:1").await);
		assert_eq!(pick(&batch[0], "doubled"), Value::from(6));
	}

	#[tokio::test]
	async fn a_field_cut_by_a_field_permission_is_invisible_to_the_where_predicate() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE FIELD secret ON t TYPE string PERMISSIONS FOR select NONE;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let table = TableName::from("t");
		let checked = build_field_state(&ctx, &table, true, None).await.unwrap();
		assert_eq!(checked.field_permissions.len(), 1);

		let predicate = physical_expr("secret = 'x'", &ctx).await;

		// Field permissions run before the condition, so the restricted field is
		// already gone when the predicate reads it and the row does not match.
		let mut batch = rows(&["{ id: t:1, secret: 'x' }"]).await;
		filter_and_process_batch(
			&mut batch,
			&PhysicalPermission::Allow,
			Some(&predicate),
			&ctx,
			&checked,
			true,
		)
		.await
		.unwrap();
		assert!(batch.is_empty());

		// With enforcement off the same predicate matches, which is what makes
		// the ordering above observable rather than incidental.
		let unchecked = build_field_state(&ctx, &table, false, None).await.unwrap();
		let mut batch = rows(&["{ id: t:1, secret: 'x' }"]).await;
		filter_and_process_batch(
			&mut batch,
			&PhysicalPermission::Allow,
			Some(&predicate),
			&ctx,
			&unchecked,
			false,
		)
		.await
		.unwrap();
		assert_eq!(batch.len(), 1);
		assert_eq!(pick(&batch[0], "secret"), Value::from("x"));

		// A row that survives on other grounds still comes out with the
		// restricted field removed.
		let survives = physical_expr("id = t:1", &ctx).await;
		let mut batch = rows(&["{ id: t:1, secret: 'x' }"]).await;
		filter_and_process_batch(
			&mut batch,
			&PhysicalPermission::Allow,
			Some(&survives),
			&ctx,
			&checked,
			true,
		)
		.await
		.unwrap();
		assert_eq!(batch.len(), 1);
		assert_eq!(pick(&batch[0], "secret"), Value::None);
	}

	#[tokio::test]
	async fn the_predicate_only_fast_path_matches_the_slow_path_and_keeps_row_order() {
		let ctx = root_ctx();
		let predicate = physical_expr("n % 2 = 1", &ctx).await;
		let empty = FieldState::empty();
		let input =
			["{ n: 0 }", "{ n: 1 }", "{ n: 2 }", "{ n: 3 }", "{ n: 4 }", "{ n: 5 }", "{ n: 6 }"];

		// Fast path: Allow + no computed fields + no field permissions, so the
		// batch is evaluated through `evaluate_batch`.
		let mut fast = rows(&input).await;
		filter_and_process_batch(
			&mut fast,
			&PhysicalPermission::Allow,
			Some(&predicate),
			&ctx,
			&empty,
			true,
		)
		.await
		.unwrap();

		// Slow path: an always-true table permission changes nothing about which
		// rows match, but forces the per-row loop.
		let allow_all = PhysicalPermission::Conditional(physical_expr("true", &ctx).await);
		let mut slow = rows(&input).await;
		filter_and_process_batch(&mut slow, &allow_all, Some(&predicate), &ctx, &empty, true)
			.await
			.unwrap();

		// Both compactions are in-place swaps of the surviving prefix, which is
		// what makes positional START/LIMIT pushdown sound.
		let expected = vec![Value::from(1), Value::from(3), Value::from(5)];
		assert_eq!(ns(&fast), expected);
		assert_eq!(ns(&slow), expected);
	}

	// =========================================================================
	// compute_fields_for_value
	// =========================================================================

	#[tokio::test]
	async fn computed_fields_are_evaluated_in_dependency_order() {
		// `y` reads `z`, so `z` must be evaluated first even though the
		// catalog hands the fields back with `y` ahead of `z`.
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE FIELD base ON t TYPE int;
			 DEFINE FIELD y ON t TYPE int COMPUTED z * 10;
			 DEFINE FIELD z ON t TYPE int COMPUTED base + 1;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let field_state =
			build_field_state(&ctx, &TableName::from("t"), false, None).await.unwrap();
		assert_eq!(field_state.computed_fields.len(), 2);
		assert_eq!(field_state.computed_fields[0].field_name(), "z");

		let mut row = val("{ id: t:1, base: 1 }").await;
		compute_fields_for_value(&ctx, &field_state, &mut row, false).await.unwrap();
		assert_eq!(pick(&row, "z"), Value::from(2));
		assert_eq!(pick(&row, "y"), Value::from(20));
	}

	#[tokio::test]
	async fn a_computed_field_is_coerced_to_its_declared_kind() {
		let ctx = root_ctx();
		let field_state = state(
			vec![computed("ratio", physical_expr("1", &ctx).await, Some(crate::expr::Kind::Float))],
			Vec::new(),
		);

		let mut row = val("{ id: t:1 }").await;
		compute_fields_for_value(&ctx, &field_state, &mut row, false).await.unwrap();
		// An integer body under `TYPE float` is stored as a float, not as the
		// integer the expression produced.
		assert!(
			matches!(pick(&row, "ratio"), Value::Number(Number::Float(f)) if f == 1.0),
			"expected a float, got {:?}",
			pick(&row, "ratio")
		);
	}

	#[tokio::test]
	async fn a_failed_coercion_surfaces_as_an_error_naming_the_field() {
		let ctx = root_ctx();
		let field_state = state(
			vec![computed(
				"count",
				physical_expr("'abc'", &ctx).await,
				Some(crate::expr::Kind::Int),
			)],
			Vec::new(),
		);

		let mut row = val("{ id: t:1 }").await;
		let err = compute_fields_for_value(&ctx, &field_state, &mut row, false).await.unwrap_err();
		let message = err.to_string();
		assert!(
			message.contains("Failed to coerce computed field 'count'"),
			"expected the coercion context, got {message}"
		);
	}

	#[tokio::test]
	async fn a_return_out_of_a_computed_body_becomes_the_field_value() {
		let ctx = root_ctx();
		// A body whose block returns rather than falling off its end signals
		// `ControlFlow::Return`, which the field takes as its value.
		let field_state = state(
			vec![computed("answer", physical_expr("{ RETURN 5 }", &ctx).await, None)],
			Vec::new(),
		);

		let mut row = val("{ id: t:1 }").await;
		compute_fields_for_value(&ctx, &field_state, &mut row, false).await.unwrap();
		assert_eq!(pick(&row, "answer"), Value::from(5));
	}

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

	#[tokio::test]
	async fn computing_record_is_taken_from_the_rows_id_so_self_reads_stay_raw() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE FIELD c ON t TYPE int COMPUTED 7;
			 DEFINE FIELD self_c ON t TYPE any COMPUTED id.c;
			 DEFINE FIELD other_c ON t TYPE any COMPUTED (t:two).c;
			 CREATE t:one;
			 CREATE t:two;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let field_state =
			build_field_state(&ctx, &TableName::from("t"), false, None).await.unwrap();

		let mut row = val("{ id: t:one }").await;
		compute_fields_for_value(&ctx, &field_state, &mut row, false).await.unwrap();

		// `computing_record` is set from the row's `id`, so dereferencing that
		// same record reads the stored data and does not re-enter computation:
		// `c` is not stored, so the self-read yields NONE.
		assert_eq!(pick(&row, "self_c"), Value::None);
		// A different record is fetched normally, computed fields included,
		// which is what makes the self-read above observably different.
		assert_eq!(pick(&row, "other_c"), Value::from(7));
	}

	// =========================================================================
	// filter_fields_by_permission
	// =========================================================================

	#[tokio::test]
	async fn a_deny_field_permission_cuts_the_field() {
		let ctx = root_ctx();
		let field_state =
			state(Vec::new(), vec![(parse_idiom("secret"), PhysicalPermission::Deny)]);

		let mut row = val("{ id: t:1, secret: 'x', public: 'y' }").await;
		filter_fields_by_permission(&ctx, &field_state, &mut row).await.unwrap();
		assert_eq!(pick(&row, "secret"), Value::None);
		assert_eq!(pick(&row, "public"), Value::from("y"));

		// An `Allow` entry is a no-op, and a non-object row is left alone.
		let allow = state(Vec::new(), vec![(parse_idiom("secret"), PhysicalPermission::Allow)]);
		let mut row = val("{ secret: 'x' }").await;
		filter_fields_by_permission(&ctx, &allow, &mut row).await.unwrap();
		assert_eq!(pick(&row, "secret"), Value::from("x"));

		let mut scalar = Value::from(1);
		filter_fields_by_permission(&ctx, &field_state, &mut scalar).await.unwrap();
		assert_eq!(scalar, Value::from(1));
	}

	#[tokio::test]
	async fn a_conditional_field_permission_decides_from_the_picked_field_value() {
		let ctx = root_ctx();
		let field_state = state(
			Vec::new(),
			vec![(
				parse_idiom("score"),
				PhysicalPermission::Conditional(physical_expr("$value > 10", &ctx).await),
			)],
		);

		let mut kept = val("{ score: 42 }").await;
		filter_fields_by_permission(&ctx, &field_state, &mut kept).await.unwrap();
		assert_eq!(pick(&kept, "score"), Value::from(42));

		let mut cut = val("{ score: 3 }").await;
		filter_fields_by_permission(&ctx, &field_state, &mut cut).await.unwrap();
		assert_eq!(pick(&cut, "score"), Value::None);
	}

	#[tokio::test]
	async fn a_field_permission_predicate_reads_the_pre_mutation_snapshot() {
		let ctx = root_ctx();
		// `a` is cut first; `b`'s predicate still reads `a` and must see the
		// value the row had before any cut, otherwise an earlier decision would
		// silently change a later one.
		let field_state = state(
			Vec::new(),
			vec![
				(parse_idiom("a"), PhysicalPermission::Deny),
				(
					parse_idiom("b"),
					PhysicalPermission::Conditional(physical_expr("a = 1", &ctx).await),
				),
			],
		);

		let mut row = val("{ a: 1, b: 2 }").await;
		filter_fields_by_permission(&ctx, &field_state, &mut row).await.unwrap();
		assert_eq!(pick(&row, "a"), Value::None);
		assert_eq!(pick(&row, "b"), Value::from(2));
	}

	#[tokio::test]
	async fn every_element_a_wildcard_deny_targets_is_cut() {
		let ctx = root_ctx();
		let field_state =
			state(Vec::new(), vec![(parse_idiom("items[*]"), PhysicalPermission::Deny)]);

		let mut row = val("{ items: [1, 2, 3, 4, 5] }").await;
		filter_fields_by_permission(&ctx, &field_state, &mut row).await.unwrap();
		// Removal walks the expanded paths in reverse, so no pending index is
		// invalidated by an earlier removal and nothing is left behind.
		assert_eq!(pick(&row, "items"), val("[]").await);
	}

	#[tokio::test]
	async fn a_wildcard_conditional_permission_cuts_every_rejected_element() {
		let ctx = root_ctx();
		let field_state = state(
			Vec::new(),
			vec![(
				parse_idiom("items[*]"),
				PhysicalPermission::Conditional(physical_expr("$value % 2 = 0", &ctx).await),
			)],
		);

		// The rejected elements sit at indices 0, 2 and 4. A forward pass would
		// shift the survivors down under each removal and leave odd values in.
		let mut row = val("{ items: [1, 2, 3, 4, 5, 6] }").await;
		filter_fields_by_permission(&ctx, &field_state, &mut row).await.unwrap();
		assert_eq!(pick(&row, "items"), val("[2, 4, 6]").await);
	}

	// =========================================================================
	// filter_field_state_for_projection
	// =========================================================================

	/// A full state with two independent computed fields (`flag`, `other`), one
	/// restricted field, and `flag` named as a field-permission dependency.
	async fn projection_state(permission_deps_complete: bool) -> FieldState {
		let ctx = root_ctx();
		let mut dep_map = HashMap::new();
		dep_map.insert(
			"flag".to_owned(),
			ComputedDeps {
				fields: vec!["score".to_owned()],
				is_complete: true,
			},
		);
		dep_map.insert(
			"other".to_owned(),
			ComputedDeps {
				fields: Vec::new(),
				is_complete: true,
			},
		);

		FieldState {
			computed_fields: vec![
				computed("flag", physical_expr("score >= 10", &ctx).await, None),
				computed("other", physical_expr("1", &ctx).await, None),
			],
			field_permissions: Arc::new(vec![
				(
					parse_idiom("secret"),
					PhysicalPermission::Conditional(physical_expr("flag", &ctx).await),
				),
				(parse_idiom("hidden"), PhysicalPermission::Deny),
			]),
			dep_map: Arc::new(dep_map),
			permission_field_deps: Arc::new(HashSet::from(["flag".to_owned()])),
			permission_deps_complete,
		}
	}

	fn computed_names(state: &FieldState) -> Vec<&str> {
		state.computed_fields.iter().map(|cf| cf.field_name()).collect()
	}

	#[tokio::test]
	async fn no_projection_keeps_every_computed_field() {
		let full = projection_state(true).await;
		let filtered = filter_field_state_for_projection(&full, None);
		assert_eq!(computed_names(&filtered), vec!["flag", "other"]);
	}

	#[tokio::test]
	async fn a_selective_projection_drops_the_computed_fields_it_does_not_need() {
		let mut full = projection_state(true).await;
		// Without a permission dependency, `SELECT other` needs only `other`.
		full.permission_field_deps = Arc::new(HashSet::new());
		let needed = HashSet::from(["other".to_owned()]);
		let filtered = filter_field_state_for_projection(&full, Some(&needed));
		assert_eq!(computed_names(&filtered), vec!["other"]);
	}

	#[tokio::test]
	async fn a_selective_projection_still_computes_fields_a_field_permission_reads() {
		let full = projection_state(true).await;
		// `SELECT secret` does not mention `flag`, but the permission on
		// `secret` reads it, so the permission decision cannot be made against
		// a row that lacks it.
		let needed = HashSet::from(["secret".to_owned()]);
		let filtered = filter_field_state_for_projection(&full, Some(&needed));
		assert_eq!(computed_names(&filtered), vec!["flag"]);
	}

	#[tokio::test]
	async fn incomplete_permission_dependencies_force_every_computed_field() {
		let full = projection_state(false).await;
		let needed = HashSet::from(["unrelated".to_owned()]);
		let filtered = filter_field_state_for_projection(&full, Some(&needed));
		assert_eq!(computed_names(&filtered), vec!["flag", "other"]);
	}

	#[tokio::test]
	async fn field_permissions_are_never_filtered_by_the_projection() {
		let full = projection_state(true).await;
		// A restricted field may be referenced only by WHERE or ORDER BY, and
		// the value-ordering guard reads this same list, so every entry has to
		// survive however narrow the projection is.
		let needed = HashSet::new();
		let filtered = filter_field_state_for_projection(&full, Some(&needed));
		assert_eq!(filtered.field_permissions.len(), 2);
		assert_eq!(filter_field_state_for_projection(&full, None).field_permissions.len(), 2);
	}

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
	async fn computed_fields_and_field_permissions_come_from_the_catalog() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE FIELD score ON t TYPE int;
			 DEFINE FIELD doubled ON t TYPE int COMPUTED score * 2;
			 DEFINE FIELD secret ON t TYPE string PERMISSIONS FOR select NONE;
			 DEFINE FIELD gated ON t TYPE string PERMISSIONS FOR select WHERE score > 1;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let table = TableName::from("t");

		let checked = build_field_state(&ctx, &table, true, None).await.unwrap();
		assert_eq!(computed_names(&checked), vec!["doubled"]);
		// `Permission::Full` fields need no runtime check and are not listed. The
		// entries follow the catalog's field order, which is by field name.
		let listed: Vec<String> =
			checked.field_permissions.iter().map(|(idiom, _)| idiom.to_raw_string()).collect();
		assert_eq!(listed, vec!["gated".to_owned(), "secret".to_owned()]);
		assert!(matches!(checked.field_permissions[0].1, PhysicalPermission::Conditional(_)));
		assert!(matches!(checked.field_permissions[1].1, PhysicalPermission::Deny));
		// `score` is read by the conditional permission, so it is recorded as a
		// dependency and the analysis is complete.
		assert!(checked.permission_deps_complete);
		assert!(checked.permission_field_deps.contains("score"));

		// Field permissions are omitted entirely when enforcement is off, while
		// computed fields still have to be evaluated.
		let unchecked = build_field_state(&ctx, &table, false, None).await.unwrap();
		assert_eq!(computed_names(&unchecked), vec!["doubled"]);
		assert!(unchecked.field_permissions.is_empty());
	}

	#[tokio::test]
	async fn field_state_is_cached_per_table_and_check_perms_flag() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE FIELD score ON t TYPE int;
			 DEFINE FIELD doubled ON t TYPE int COMPUTED score * 2;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let db_ctx = ctx.database().unwrap();
		let table = TableName::from("t");

		build_field_state(&ctx, &table, true, None).await.unwrap();
		build_field_state(&ctx, &table, true, None).await.unwrap();
		{
			let cache = db_ctx.field_state_cache.read().await;
			assert_eq!(cache.len(), 1);
			assert!(cache.contains_key(&(table.clone(), true)));
		}

		// The flag is part of the key: the two states differ, so they cannot
		// share an entry.
		build_field_state(&ctx, &table, false, None).await.unwrap();
		let cache = db_ctx.field_state_cache.read().await;
		assert_eq!(cache.len(), 2);
		assert!(cache.contains_key(&(table, false)));
	}

	#[tokio::test]
	async fn a_projection_filters_the_cached_state_without_narrowing_the_cache() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE FIELD score ON t TYPE int;
			 DEFINE FIELD doubled ON t TYPE int COMPUTED score * 2;
			 DEFINE FIELD tripled ON t TYPE int COMPUTED score * 3;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let table = TableName::from("t");

		let needed = HashSet::from(["doubled".to_owned()]);
		let projected = build_field_state(&ctx, &table, false, Some(&needed)).await.unwrap();
		assert_eq!(computed_names(&projected), vec!["doubled"]);

		// The cached entry keeps both computed fields, so the next query with a
		// different projection is served correctly from the same entry.
		let full = build_field_state(&ctx, &table, false, None).await.unwrap();
		assert_eq!(full.computed_fields.len(), 2);
	}

	// =========================================================================
	// eval_limit_expr
	// =========================================================================

	#[tokio::test]
	async fn a_non_negative_integer_limit_evaluates_to_itself() {
		let ctx = root_ctx();
		let expr = physical_expr("7", &ctx).await;
		assert_eq!(eval_limit_expr(expr.as_ref(), &ctx).await.unwrap(), 7);

		let zero = physical_expr("0", &ctx).await;
		assert_eq!(eval_limit_expr(zero.as_ref(), &ctx).await.unwrap(), 0);
	}

	#[tokio::test]
	async fn an_absent_limit_means_no_offset() {
		let ctx = root_ctx();
		for src in ["NONE", "NULL"] {
			let expr = physical_expr(src, &ctx).await;
			assert_eq!(
				eval_limit_expr(expr.as_ref(), &ctx).await.unwrap(),
				0,
				"{src} should evaluate to 0"
			);
		}
	}

	#[tokio::test]
	async fn a_negative_limit_is_rejected() {
		let ctx = root_ctx();
		let expr = physical_expr("-1", &ctx).await;
		let err = eval_limit_expr(expr.as_ref(), &ctx).await.unwrap_err();
		assert!(
			err.to_string().contains("non-negative"),
			"expected the non-negative message, got {err}"
		);
	}

	#[tokio::test]
	async fn a_non_numeric_limit_is_rejected() {
		let ctx = root_ctx();
		let expr = physical_expr("'ten'", &ctx).await;
		let err = eval_limit_expr(expr.as_ref(), &ctx).await.unwrap_err();
		assert!(
			err.to_string().contains("must be an integer"),
			"expected the integer message, got {err}"
		);
	}

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
		let name_then_id = Ordering::Order(OrderList(vec![
			Order {
				value: parse_idiom("name"),
				direction: false,
				..Default::default()
			},
			Order {
				value: parse_idiom("id"),
				direction: false,
				..Default::default()
			},
		]));
		assert_eq!(determine_scan_direction(Some(&name_then_id)), Direction::Forward);

		// No ORDER BY, an empty list, and ORDER BY RAND() all scan forward.
		assert_eq!(determine_scan_direction(None), Direction::Forward);
		let empty = Ordering::Order(OrderList(Vec::new()));
		assert_eq!(determine_scan_direction(Some(&empty)), Direction::Forward);
		assert_eq!(determine_scan_direction(Some(&Ordering::Random)), Direction::Forward);
	}

	// =========================================================================
	// decode_record
	// =========================================================================

	#[tokio::test]
	async fn a_stored_record_decodes_with_its_id_taken_from_the_key() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			 CREATE t:tobie SET name = 'Tobie', age = 30;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let db_ctx = ctx.database().unwrap();
		let table = TableName::from("t");

		let range = RecordPrefix {
			ns: db_ctx.ns_ctx.ns.namespace_id,
			db: db_ctx.db.database_id,
			tb: Cow::Borrowed(&table),
		}
		.range()
		.unwrap();
		let raw = ctx.txn().scan_raw(range, 10, 0, None).await.unwrap();
		assert_eq!(raw.len(), 1, "the table holds exactly one record");

		let (key, value) = &raw[0];
		let decoded = decode_record(key, value).unwrap();
		assert_eq!(pick(&decoded, "name"), Value::from("Tobie"));
		assert_eq!(pick(&decoded, "age"), Value::from(30));
		// The `id` is rebuilt from the key rather than trusted from the value.
		assert_eq!(pick(&decoded, "id"), val("t:tobie").await);

		// A key that is not a record key cannot be decoded.
		assert!(decode_record(b"not-a-record-key", value).is_err());
	}
}
