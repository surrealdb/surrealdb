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
use std::ops::Bound;
use std::sync::Arc;

use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::exec::permission::{
	PhysicalPermission, check_permission_for_value, convert_permission_to_physical,
};
use crate::exec::pre_decode_filter::{PreDecodeFilter, PreDecodeFilterOutcome};
use crate::exec::{EvalContext, ExecutionContext, PhysicalExpr, ValueBatch, ValueBatchStream};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::idx::planner::ScanDirection;
use crate::key::record;
use crate::kvs::{KVKey, KVValue, Transaction};
use crate::val::{RecordIdKey, TableName, Value};

/// A raw computed field entry before topological sorting:
/// `(field_name, physical_expr, optional_kind, dependency_field_names)`.
type RawComputedField = (String, Arc<dyn PhysicalExpr>, Option<crate::expr::Kind>, Vec<String>);

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
pub(crate) fn determine_scan_direction(
	order: Option<&crate::expr::order::Ordering>,
) -> ScanDirection {
	use crate::expr::order::Ordering as OrderingType;
	if let Some(OrderingType::Order(order_list)) = order
		&& let Some(first) = order_list.0.first()
		&& !first.direction
		&& first.value.is_id()
	{
		ScanDirection::Backward
	} else {
		ScanDirection::Forward
	}
}

// =============================================================================
// KV scan stream
// =============================================================================

/// Produce a `ValueBatchStream` from a raw KV range scan.
///
/// When `pre_skip > 0`, that many entries are skipped at the KV storage layer
/// before any data is returned, avoiding I/O, allocation, and deserialization
/// for rows that will be discarded anyway (the fast-path optimisation for
/// `START` without a pushdown predicate).
///
/// When `limit_hint` is provided, the first batch is capped to that count so
/// small-limit queries (e.g. `LIMIT 10`) don't fetch 500 records from
/// storage. Subsequent batches use [`crate::kvs::NORMAL_BATCH_SIZE`].
///
/// Iterates the cursor's borrowed `&[u8]` slices directly — record decode
/// happens inline, no intermediate owned `Vec<u8>` allocation per row.
#[allow(clippy::too_many_arguments)]
pub(crate) fn kv_scan_stream(
	txn: Arc<Transaction>,
	beg: crate::kvs::Key,
	end: crate::kvs::Key,
	version: Option<u64>,
	storage_limit: Option<usize>,
	direction: ScanDirection,
	pre_skip: usize,
	limit_hint: Option<u32>,
	pre_decode_filter: Option<Arc<PreDecodeFilter>>,
) -> ValueBatchStream {
	let skip = pre_skip.min(u32::MAX as usize) as u32;
	let stream = async_stream::try_stream! {
		let mut cursor = txn
			.open_vals_cursor(beg..end, direction, skip, version)
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
			if first
				&& let Some(h) = limit_hint
			{
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
			let batch = cursor
				.next_batch(crate::kvs::ScanLimit::Count(batch_size))
				.await
				.context("Failed to scan record")?;
			if batch.is_empty() {
				break;
			}
			let mut decoded = Vec::with_capacity(batch.len());
			// Hoist the pre-decode-filter branch out of the per-item loop:
			// `pre_decode_filter` is `Option<Arc<…>>` and doesn't change
			// across iterations, so we pay the `Option`-check once per
			// batch instead of per row.
			match &pre_decode_filter {
				Some(pdf) => {
					for (key, val) in &batch {
						if pdf.apply(key, val) == PreDecodeFilterOutcome::Reject {
							continue;
						}
						decoded.push(decode_record(key, val)?);
					}
				}
				None => {
					for (key, val) in &batch {
						decoded.push(decode_record(key, val)?);
					}
				}
			}
			first = false;
			yielded += batch.len();
			if !decoded.is_empty() {
				yield ValueBatch { values: decoded };
			}
		}
	};
	Box::pin(stream)
}

/// Decode a record from its key and value bytes.
#[inline]
pub(crate) fn decode_record(key: &[u8], val: &[u8]) -> Result<Value, ControlFlow> {
	let decoded_key =
		crate::key::record::RecordKey::decode_key(key).context("Failed to decode record key")?;

	let rid = crate::val::RecordId {
		table: decoded_key.tb.into_owned(),
		key: decoded_key.id,
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
					let mut eval_ctx = EvalContext::from_exec_ctx($ctx).with_value($value);
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
///   table permission -> computed fields -> WHERE predicate -> field permissions.
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

/// Compute the start key for a range scan.
pub(crate) fn range_start_key(
	ns_id: NamespaceId,
	db_id: DatabaseId,
	table: &TableName,
	bound: &Bound<RecordIdKey>,
) -> Result<crate::kvs::Key, ControlFlow> {
	match bound {
		Bound::Unbounded => {
			record::prefix(ns_id, db_id, table).context("Failed to create prefix key")
		}
		Bound::Included(v) => {
			record::new(ns_id, db_id, table, v).encode_key().context("Failed to create begin key")
		}
		Bound::Excluded(v) => {
			let mut key = record::new(ns_id, db_id, table, v)
				.encode_key()
				.context("Failed to create begin key")?;
			key.push(0x00);
			Ok(key)
		}
	}
}

/// Compute the end key for a range scan.
pub(crate) fn range_end_key(
	ns_id: NamespaceId,
	db_id: DatabaseId,
	table: &TableName,
	bound: &Bound<RecordIdKey>,
) -> Result<crate::kvs::Key, ControlFlow> {
	match bound {
		Bound::Unbounded => {
			record::suffix(ns_id, db_id, table).context("Failed to create suffix key")
		}
		Bound::Excluded(v) => {
			record::new(ns_id, db_id, table, v).encode_key().context("Failed to create end key")
		}
		Bound::Included(v) => {
			let mut key = record::new(ns_id, db_id, table, v)
				.encode_key()
				.context("Failed to create end key")?;
			key.push(0x00);
			Ok(key)
		}
	}
}

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
}

impl ComputedFieldDef {
	/// Root field name this computed-field definition is attached to.
	pub(crate) fn field_name(&self) -> &str {
		&self.field_name
	}
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
		&& field_defs
			.iter()
			.any(|fd| !matches!(fd.select_permission, crate::catalog::Permission::Full));
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

			let deps = if let Some(ref cd) = fd.computed_deps {
				crate::expr::computed_deps::ComputedDeps {
					fields: cd.fields.clone(),
					is_complete: cd.is_complete,
				}
			} else {
				crate::expr::computed_deps::extract_computed_deps(expr)
			};

			dep_map.insert(field_name.clone(), deps.clone());

			let physical_expr = planner.physical_expr(expr.clone()).await.with_context(|| {
				format!("Computed field '{field_name}' has unsupported expression")
			})?;

			raw_computed.push((field_name, physical_expr, fd.field_kind.clone(), deps.fields));
		}
	}

	// Topologically sort ALL computed fields for correct evaluation order
	let topo_input: Vec<(String, Vec<String>)> =
		raw_computed.iter().map(|(name, _, _, deps)| (name.clone(), deps.clone())).collect();
	let sorted_indices = crate::expr::computed_deps::topological_sort_computed_fields(&topo_input);

	let mut computed_fields = Vec::with_capacity(sorted_indices.len());
	for idx in sorted_indices {
		let (field_name, expr, kind, _) = &raw_computed[idx];
		computed_fields.push(ComputedFieldDef {
			field_name: field_name.clone(),
			expr: Arc::clone(expr),
			kind: kind.clone(),
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
			if matches!(fd.select_permission, crate::catalog::Permission::Full) {
				continue;
			}
			if let crate::catalog::Permission::Specific(ref expr) = fd.select_permission {
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
		// Evaluate with the row as both current value and document root so
		// nested subqueries see the same `$parent` as top-level projections (#7154).
		let row_ctx = eval_ctx.with_value_and_doc(value);
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
				for path in original.each(&idiom.0) {
					value.cut(&path.0);
				}
			}
			PhysicalPermission::Conditional(_) => {
				let original = snapshot.get_or_insert_with(|| value.clone());
				for path in original.each(&idiom.0) {
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
