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

use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;

use futures::StreamExt;

use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::exec::permission::{
	PhysicalPermission, check_permission_for_value, convert_permission_to_physical,
};
use crate::exec::planner::expr_to_physical_expr;
use crate::exec::{EvalContext, ExecutionContext, PhysicalExpr, ValueBatch, ValueBatchStream};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::idx::planner::ScanDirection;
use crate::key::record;
use crate::kvs::{KVKey, KVValue, Transaction};
use crate::val::{RecordIdKey, TableName, Value};

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
	pub(crate) fn new(
		permission: PhysicalPermission,
		predicate: Option<Arc<dyn PhysicalExpr>>,
		field_state: FieldState,
		check_perms: bool,
		limit: Option<usize>,
		start: usize,
	) -> Self {
		let needs_processing = !matches!(permission, PhysicalPermission::Allow)
			|| !field_state.computed_fields.is_empty()
			|| (check_perms && !field_state.field_permissions.is_empty())
			|| predicate.is_some();
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
	order: &Option<crate::expr::order::Ordering>,
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
#[allow(clippy::too_many_arguments)]
pub(crate) fn kv_scan_stream(
	txn: Arc<Transaction>,
	beg: crate::kvs::Key,
	end: crate::kvs::Key,
	version: Option<u64>,
	storage_limit: Option<usize>,
	direction: ScanDirection,
	pre_skip: usize,
	prefetch: bool,
) -> ValueBatchStream {
	let skip = pre_skip.min(u32::MAX as usize) as u32;
	let stream = async_stream::try_stream! {
		let kv_stream = txn.stream_keys_vals(beg..end, version, storage_limit, skip, direction, prefetch);
		futures::pin_mut!(kv_stream);

		while let Some(result) = kv_stream.next().await {
			let entries = result.context("Failed to scan record")?;
			let mut batch = Vec::with_capacity(entries.len());
			for (key, val) in entries {
				batch.push(decode_record(&key, val)?);
			}
			if !batch.is_empty() {
				yield ValueBatch { values: batch };
			}
		}
	};
	Box::pin(stream)
}

/// Decode a record from its key and value bytes.
#[inline]
pub(crate) fn decode_record(key: &[u8], val: Vec<u8>) -> Result<Value, ControlFlow> {
	let decoded_key =
		crate::key::record::RecordKey::decode_key(key).context("Failed to decode record key")?;

	let rid = crate::val::RecordId {
		table: decoded_key.tb.into_owned(),
		key: decoded_key.id,
	};

	let mut record =
		crate::catalog::Record::kv_decode_value(val).context("Failed to deserialize record")?;

	// Inject the id field into the document
	record.data.def(rid);

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
				let eval_ctx = EvalContext::from_exec_ctx($ctx).with_value($value);
				expr.evaluate(eval_ctx).await.map(|v| v.is_truthy()).map_err(|e| {
					ControlFlow::Err(anyhow::anyhow!("Failed to check permission: {e}"))
				})
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
		compute_fields_for_value(ctx, state, &mut batch[write_idx]).await?;
		// Field-level permissions (must run before the WHERE predicate so that
		// restricted fields are removed before the condition is evaluated,
		// matching the old compute path's behaviour).
		if check_perms {
			filter_fields_by_permission(ctx, state, &mut batch[write_idx]).await?;
		}
		// WHERE predicate (evaluated on the permission-reduced document)
		if let Some(pred) = predicate {
			let eval_ctx = EvalContext::from_exec_ctx(ctx).with_value(&batch[write_idx]);
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
#[derive(Debug, Clone)]
pub(crate) struct FieldState {
	/// Computed field definitions converted to physical expressions
	pub(crate) computed_fields: Vec<ComputedFieldDef>,
	/// Field-level permissions (field name -> permission)
	pub(crate) field_permissions: HashMap<String, PhysicalPermission>,
	/// Dependency map for computed fields, used for projection filtering.
	/// Stored alongside the cached state so that projected queries can
	/// cheaply determine the subset of computed fields they need.
	dep_map: HashMap<String, crate::expr::computed_deps::ComputedDeps>,
}

impl FieldState {
	/// Create an empty field state with no computed fields or field permissions.
	pub(crate) fn empty() -> Self {
		Self {
			computed_fields: Vec::new(),
			field_permissions: HashMap::new(),
			dep_map: HashMap::new(),
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

/// Build field state from raw transaction and context parameters.
///
/// This is the core implementation that does the actual work: KV lookup of
/// field definitions, PhysicalExpr compilation, dependency analysis, and
/// topological sorting. It takes explicit parameters instead of
/// `ExecutionContext`, making it usable at both plan time and execution time.
pub(crate) async fn build_field_state_raw(
	txn: &Transaction,
	ctx: &crate::ctx::FrozenContext,
	ns_id: crate::catalog::NamespaceId,
	db_id: crate::catalog::DatabaseId,
	table_name: &TableName,
	check_perms: bool,
) -> Result<FieldState, ControlFlow> {
	let field_defs = txn
		.all_tb_fields(ns_id, db_id, table_name, None)
		.await
		.context("Failed to get field definitions")?;

	// Fast path: if there are no computed fields and no field-level permissions
	// that need checking, skip the expensive resolution.
	let has_computed = field_defs.iter().any(|fd| fd.computed.is_some());
	let has_field_perms =
		check_perms && field_defs.iter().any(|fd| fd.select_permission.is_specific());
	if !has_computed && !has_field_perms {
		return Ok(FieldState::empty());
	}

	// Collect ALL computed fields and their dependency metadata.
	let mut raw_computed: Vec<(
		String,
		Arc<dyn PhysicalExpr>,
		Option<crate::expr::Kind>,
		Vec<String>,
	)> = Vec::new();
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

			let physical_expr =
				expr_to_physical_expr(expr.clone(), ctx).await.with_context(|| {
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

	// Build field permissions
	let mut field_permissions = HashMap::new();
	if check_perms {
		for fd in field_defs.iter() {
			let field_name = fd.name.to_raw_string();
			let physical_perm = convert_permission_to_physical(&fd.select_permission, ctx)
				.await
				.context("Failed to convert field permission")?;
			field_permissions.insert(field_name, physical_perm);
		}
	}

	Ok(FieldState {
		computed_fields,
		field_permissions,
		dep_map,
	})
}

/// Fetch field definitions and build the cached field state.
///
/// Always builds and caches the *full* field state (all computed fields and
/// permissions) keyed by `(table, check_perms)`. When `needed_fields` is
/// `Some`, the cached full state is cheaply filtered to the required subset.
/// This avoids repeated expensive work (KV lookups, PhysicalExpr compilation,
/// dependency analysis, topological sort) for projected queries.
#[allow(clippy::type_complexity)]
pub(crate) async fn build_field_state(
	ctx: &ExecutionContext,
	table_name: &TableName,
	check_perms: bool,
	needed_fields: Option<&std::collections::HashSet<String>>,
) -> Result<FieldState, ControlFlow> {
	let db_ctx = ctx.database().context("build_field_state requires database context")?;
	let cache_key = (table_name.clone(), check_perms);

	// Check the cache first (keyed by table name + check_perms flag).
	if let Ok(cache) = db_ctx.field_state_cache.read() {
		if let Some(cached) = cache.get(&cache_key) {
			return Ok(filter_field_state_for_projection(cached, needed_fields));
		}
	}

	// Delegate to the raw implementation
	let full_state = build_field_state_raw(
		&ctx.txn(),
		ctx.ctx(),
		db_ctx.ns_ctx.ns.namespace_id,
		db_ctx.db.database_id,
		table_name,
		check_perms,
	)
	.await?;

	// Cache the full (unfiltered) state
	let cached = Arc::new(full_state);
	if let Ok(mut cache) = db_ctx.field_state_cache.write() {
		cache.insert(cache_key, Arc::clone(&cached));
	}

	// Return filtered if needed_fields is specified
	Ok(filter_field_state_for_projection(&cached, needed_fields))
}

/// Filter a full FieldState down to only the computed fields required by
/// the given projection. When `needed_fields` is None (SELECT *), returns
/// a clone of the full state. This is a cheap CPU-only operation with no
/// KV lookups.
pub(crate) fn filter_field_state_for_projection(
	full_state: &FieldState,
	needed_fields: Option<&std::collections::HashSet<String>>,
) -> FieldState {
	let Some(needed) = needed_fields else {
		return full_state.clone();
	};

	// Determine which computed fields are required by the projection
	let required =
		crate::expr::computed_deps::resolve_required_computed_fields(needed, &full_state.dep_map);

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
		field_permissions: full_state.field_permissions.clone(),
		dep_map: full_state.dep_map.clone(),
	}
}

/// Compute all computed fields for a single value.
pub(crate) async fn compute_fields_for_value(
	ctx: &ExecutionContext,
	state: &FieldState,
	value: &mut Value,
) -> Result<(), ControlFlow> {
	if state.computed_fields.is_empty() {
		return Ok(());
	}

	let eval_ctx = EvalContext::from_exec_ctx(ctx);

	for cf in &state.computed_fields {
		// Evaluate with the current value as context
		let row_ctx = eval_ctx.with_value(value);
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
pub(crate) async fn filter_fields_by_permission(
	ctx: &ExecutionContext,
	state: &FieldState,
	value: &mut Value,
) -> Result<(), ControlFlow> {
	if state.field_permissions.is_empty() {
		return Ok(());
	}

	// Collect fields to check
	let field_names: Vec<String> = {
		let Value::Object(obj) = &*value else {
			return Ok(());
		};
		obj.keys().cloned().collect()
	};

	for field_name in field_names {
		// Check if there's a permission for this field
		if let Some(perm) = state.field_permissions.get(&field_name) {
			let allowed = check_permission_for_value(perm, &*value, ctx)
				.await
				.context("Failed to check field permission")?;

			if !allowed && let Value::Object(obj) = value {
				obj.remove(&field_name);
			}
		}
	}

	Ok(())
}
