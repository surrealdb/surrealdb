use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use tracing::instrument;

use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId, Permission};
use crate::err::Error;
use crate::exec::index::access_path::{AccessPath, select_access_path};
use crate::exec::index::analysis::IndexAnalyzer;
use crate::exec::operators::{FullTextScan, IndexScan};
use crate::exec::permission::{
	PhysicalPermission, check_permission_for_value, convert_permission_to_physical,
	should_check_perms, validate_record_user_access,
};
use crate::exec::planner::expr_to_physical_expr;
use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, monitor_stream,
};
use crate::expr::order::Ordering;
use crate::expr::with::With;
use crate::expr::{Cond, ControlFlow, ControlFlowExt};
use crate::iam::Action;
use crate::idx::planner::ScanDirection;
use crate::key::record;
use crate::kvs::{KVKey, KVValue, Transaction};
use crate::val::{RecordIdKey, TableName, Value};

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

/// Full table scan - iterates over all records in a table.
///
/// Requires database-level context since it reads from a specific table
/// in the selected namespace and database.
///
/// Permission checking is performed at execution time by resolving the table
/// definition from the current transaction's schema view and filtering records
/// based on the SELECT permission.
///
/// When scanning a table, this operator can perform index selection based on
/// the provided WHERE condition, ORDER BY clause, and WITH hints.
///
/// The optional `predicate`, `limit`, and `start` fields allow the planner to
/// push the Filter, Limit, and Start operators down into the scan, reducing
/// pipeline overhead and enabling early termination for `WHERE ... LIMIT`
/// queries.
#[derive(Debug, Clone)]
pub struct Scan {
	pub(crate) source: Arc<dyn PhysicalExpr>,
	/// Optional version timestamp for time-travel queries (VERSION clause)
	pub(crate) version: Option<Arc<dyn PhysicalExpr>>,
	/// Optional WHERE condition for index selection (AST form)
	pub(crate) cond: Option<Cond>,
	/// Optional ORDER BY for index selection and scan direction
	pub(crate) order: Option<Ordering>,
	/// Optional WITH INDEX/NOINDEX hints
	pub(crate) with: Option<With>,
	/// Fields needed by the query (projection + WHERE + ORDER + GROUP).
	/// `None` means all fields are needed (SELECT *).
	pub(crate) needed_fields: Option<std::collections::HashSet<String>>,
	/// Compiled WHERE predicate pushed down from the Filter operator.
	/// Applied after computed fields, before field-level permissions.
	pub(crate) predicate: Option<Arc<dyn PhysicalExpr>>,
	/// LIMIT expression pushed down from the Limit operator.
	/// Maximum number of rows to return after filtering.
	pub(crate) limit: Option<Arc<dyn PhysicalExpr>>,
	/// START offset expression pushed down from the Limit operator.
	/// Number of rows to skip (after filtering) before emitting.
	pub(crate) start: Option<Arc<dyn PhysicalExpr>>,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl Scan {
	/// Create a new Scan operator with fresh metrics.
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		source: Arc<dyn PhysicalExpr>,
		version: Option<Arc<dyn PhysicalExpr>>,
		cond: Option<Cond>,
		order: Option<Ordering>,
		with: Option<With>,
		needed_fields: Option<std::collections::HashSet<String>>,
		predicate: Option<Arc<dyn PhysicalExpr>>,
		limit: Option<Arc<dyn PhysicalExpr>>,
		start: Option<Arc<dyn PhysicalExpr>>,
	) -> Self {
		Self {
			source,
			version,
			cond,
			order,
			with,
			needed_fields,
			predicate,
			limit,
			start,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for Scan {
	fn name(&self) -> &'static str {
		"Scan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![("source".to_string(), self.source.to_sql())];
		if let Some(ref pred) = self.predicate {
			attrs.push(("predicate".to_string(), pred.to_sql()));
		}
		if let Some(ref limit) = self.limit {
			attrs.push(("limit".to_string(), limit.to_sql()));
		}
		if let Some(ref start) = self.start {
			attrs.push(("offset".to_string(), start.to_sql()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		let mut exprs = vec![("source", &self.source)];
		if let Some(ref version) = self.version {
			exprs.push(("version", version));
		}
		if let Some(ref pred) = self.predicate {
			exprs.push(("predicate", pred));
		}
		if let Some(ref limit) = self.limit {
			exprs.push(("limit", limit));
		}
		if let Some(ref start) = self.start {
			exprs.push(("start", start));
		}
		exprs
	}

	fn access_mode(&self) -> AccessMode {
		// Scan is read-only, but the source expression or predicate could contain a subquery
		// containing a mutation.
		let mut mode = self.source.access_mode();
		if let Some(ref pred) = self.predicate {
			mode = mode.combine(pred.access_mode());
		}
		if let Some(ref limit) = self.limit {
			mode = mode.combine(limit.access_mode());
		}
		if let Some(ref start) = self.start {
			mode = mode.combine(start.access_mode());
		}
		mode
	}

	#[instrument(name = "Scan::execute", level = "trace", skip_all)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		// Get database context - we declared Database level, so this should succeed
		let db_ctx = ctx.database()?.clone();

		// Validate record user has access to this namespace/database
		validate_record_user_access(&db_ctx)?;

		// Check if we need to enforce permissions
		let check_perms = should_check_perms(&db_ctx, Action::View)?;

		// Clone for the async block
		let source_expr = Arc::clone(&self.source);
		let version = self.version.clone();
		let cond = self.cond.clone();
		let order = self.order.clone();
		let with = self.with.clone();
		let needed_fields = self.needed_fields.clone();
		let predicate = self.predicate.clone();
		let limit_expr = self.limit.clone();
		let start_expr = self.start.clone();
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			let db_ctx = ctx.database().context("Scan requires database context")?;
			let txn = ctx.txn();
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);

			// Evaluate table expression
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let table_value = source_expr.evaluate(eval_ctx).await?;

			// Determine scan target: either a table name or a record ID
			let (table_name, rid) = match table_value {
				Value::Table(t) => (t, None),
				Value::RecordId(rid) => (rid.table.clone(), Some(rid)),
				Value::Array(arr) => {
					yield ValueBatch { values: arr.0 };
					return;
				}
				// For any other value type, yield as a single row.
				// This matches legacy FROM behavior for non-table values.
				other => {
					yield ValueBatch { values: vec![other] };
					return;
				}
			};

			// Evaluate pushed-down LIMIT and START expressions
			let limit_val: Option<usize> = match &limit_expr {
				Some(expr) => Some(eval_limit_expr(&**expr, &ctx).await?),
				None => None,
			};
			let start_val: usize = match &start_expr {
				Some(expr) => eval_limit_expr(&**expr, &ctx).await?,
				None => 0,
			};

			// Evaluate VERSION expression to a timestamp
			let version: Option<u64> = match &version {
				Some(expr) => {
					let eval_ctx = EvalContext::from_exec_ctx(&ctx);
					let v = expr.evaluate(eval_ctx).await?;
					Some(
						v.cast_to::<crate::val::Datetime>()
							.map_err(|e| anyhow::anyhow!("{e}"))?
							.to_version_stamp()?,
					)
				}
				None => None,
			};

			// Early exit if limit is 0
			if limit_val == Some(0) {
				return;
			}

			// Check table existence and resolve SELECT permission
			let table_def = txn
				.get_tb_by_name(&ns.name, &db.name, &table_name)
				.await
				.context("Failed to get table")?;

			if table_def.is_none() {
				Err(ControlFlow::Err(anyhow::Error::new(Error::TbNotFound {
					name: table_name.clone(),
				})))?;
			}

			let select_permission = if check_perms {
				let catalog_perm = match &table_def {
					Some(def) => def.permissions.select.clone(),
					None => Permission::None,
				};
				convert_permission_to_physical(&catalog_perm, ctx.ctx())
					.context("Failed to convert permission")?
			} else {
				PhysicalPermission::Allow
			};

			// Early exit if denied
			if matches!(select_permission, PhysicalPermission::Deny) {
				return;
			}

			// Eagerly initialize field state (computed fields + field permissions)
			let field_state = build_field_state(&ctx, &table_name, check_perms, needed_fields.as_ref()).await?;

			// Pre-compute whether any post-decode processing is needed.
			// When false, the scan loop can skip filter/process calls entirely
			// (zero async overhead beyond the KV stream poll).
			let needs_processing = !matches!(select_permission, PhysicalPermission::Allow)
				|| !field_state.computed_fields.is_empty()
				|| (check_perms && !field_state.field_permissions.is_empty())
				|| predicate.is_some();

			// === POINT LOOKUP (single record by ID) ===
			// Handled inline - single record, no limit/start or pipeline needed.
			if let Some(ref rid) = rid
				&& !matches!(rid.key, RecordIdKey::Range(_)) {
					let record = txn
						.get_record(ns.namespace_id, db.database_id, &rid.table, &rid.key, version)
						.await
						.context("Failed to get record")?;

					if record.data.as_ref().is_none() {
						return;
					}

					let mut value = record.data.as_ref().clone();
					value.def(rid);

					let mut batch = vec![value];
					if needs_processing {
						filter_and_process_batch(
							&mut batch, &select_permission, predicate.as_ref(),
							&ctx, &field_state, check_perms,
						).await?;
					}
					if !batch.is_empty() {
						yield ValueBatch { values: batch };
					}
					return;
				}

			// === STREAM-BASED PATHS (range scan and table scan) ===
			// All use the unified ScanPipeline consumption loop.

			// When no processing is needed, push start to the KV layer as pre_skip
			// so rows are discarded before deserialization.
			let pre_skip = if !needs_processing { start_val } else { 0 };

			// Push start+limit to the storage layer for the pure fast path.
			let effective_storage_limit = if !needs_processing {
				limit_val.map(|l| start_val.saturating_add(l))
			} else {
				None
			};

			let direction = determine_scan_direction(&order);

			// Create the source stream based on scan type.
			// `applied_pre_skip` tracks how many rows the source will skip
			// before decoding, so the pipeline can adjust its start accordingly.
			let (mut source, applied_pre_skip) = if let Some(rid) = rid {
				// Range scan (must be a range since point lookup returned above)
				let RecordIdKey::Range(range) = &rid.key else { unreachable!() };
				let beg = range_start_key(ns.namespace_id, db.database_id, &rid.table, &range.start)?;
				let end = range_end_key(ns.namespace_id, db.database_id, &rid.table, &range.end)?;
				let stream = kv_scan_stream(
					Arc::clone(&txn), beg, end, version,
					effective_storage_limit, direction, pre_skip,
				);
				(stream, pre_skip)
			} else {
				// Table scan (with index selection)
				resolve_table_scan_stream(
					&ctx, TableScanConfig {
						ns_id: ns.namespace_id,
						db_id: db.database_id,
						table_name,
						cond,
						order: order.clone(),
						with,
						direction,
						version,
						storage_limit: effective_storage_limit,
						pre_skip,
					},
				).await?
			};

			// Build the pipeline with start adjusted for any pre-skipping.
			let mut pipeline = ScanPipeline::new(
				select_permission, predicate, field_state,
				check_perms, limit_val, start_val.saturating_sub(applied_pre_skip),
			);

			// Unified consumption loop for all stream-based sources.
			while let Some(batch_result) = source.next().await {
				// Check for cancellation between batches
				if ctx.cancellation().is_cancelled() {
					Err(ControlFlow::Err(
						anyhow::anyhow!(crate::err::Error::QueryCancelled),
					))?;
				}
				let mut batch = batch_result?;
				let cont = pipeline.process_batch(&mut batch.values, &ctx).await?;
				if !batch.values.is_empty() {
					yield ValueBatch { values: batch.values };
				}
				if !cont {
					break;
				}
			}
		};

		Ok(monitor_stream(Box::pin(stream), "Scan", &self.metrics))
	}
}

// ---------------------------------------------------------------------------
// ScanPipeline: encapsulates per-batch processing and cross-batch limit/start
// ---------------------------------------------------------------------------

/// Inline pipeline that performs all per-batch operations (filtering, computed
/// fields, permissions, limit/start) in a single pass with minimal await
/// boundaries. Limit/start state is tracked across batches so the logic is
/// written once rather than duplicated in every scan path.
struct ScanPipeline {
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
	fn new(
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
	async fn process_batch(
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

// ---------------------------------------------------------------------------
// Source helpers
// ---------------------------------------------------------------------------

/// Determine scan direction from ORDER BY clause.
/// Returns Backward if the first ORDER BY is `id DESC`, otherwise Forward.
fn determine_scan_direction(order: &Option<Ordering>) -> ScanDirection {
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

/// Produce a `ValueBatchStream` from a raw KV range scan.
///
/// When `pre_skip > 0`, that many KV pairs are discarded *before* decoding,
/// avoiding deserialization work for rows that will be skipped anyway (the
/// fast-path optimisation for `START` without a pushdown predicate).
fn kv_scan_stream(
	txn: Arc<Transaction>,
	beg: crate::kvs::Key,
	end: crate::kvs::Key,
	version: Option<u64>,
	storage_limit: Option<usize>,
	direction: ScanDirection,
	pre_skip: usize,
) -> ValueBatchStream {
	let stream = async_stream::try_stream! {
		let kv_stream = txn.stream_keys_vals(beg..end, version, storage_limit, direction);
		futures::pin_mut!(kv_stream);

		let mut skipped = 0usize;
		while let Some(result) = kv_stream.next().await {
			let entries = result.context("Failed to scan record")?;
			// Fast path: skip entire batch when all entries fall within pre_skip
			let remaining_to_skip = pre_skip - skipped;
			if entries.len() <= remaining_to_skip {
				skipped += entries.len();
				continue;
			}
			// Allocate only for entries that will actually be decoded
			let mut batch = Vec::with_capacity(entries.len() - remaining_to_skip);
			for (key, val) in entries {
				if skipped < pre_skip {
					skipped += 1;
					continue; // discard without decoding
				}
				batch.push(decode_record(&key, val)?);
			}
			if !batch.is_empty() {
				yield ValueBatch { values: batch };
			}
		}
	};
	Box::pin(stream)
}

/// Configuration bundle for [`resolve_table_scan_stream`].
struct TableScanConfig {
	ns_id: NamespaceId,
	db_id: DatabaseId,
	table_name: TableName,
	cond: Option<Cond>,
	order: Option<Ordering>,
	with: Option<With>,
	direction: ScanDirection,
	version: Option<u64>,
	storage_limit: Option<usize>,
	/// Number of KV pairs to skip before decoding (fast-path only).
	pre_skip: usize,
}

/// Resolve the optimal access path for a table scan and return the source
/// stream together with the number of rows that were pre-skipped at the
/// KV layer (zero for index / full-text sources).
async fn resolve_table_scan_stream(
	ctx: &ExecutionContext,
	cfg: TableScanConfig,
) -> Result<(ValueBatchStream, usize), ControlFlow> {
	let txn = ctx.txn();

	let access_path = if matches!(&cfg.with, Some(With::NoIndex)) {
		None
	} else {
		let indexes = txn
			.all_tb_indexes(cfg.ns_id, cfg.db_id, &cfg.table_name)
			.await
			.context("Failed to fetch indexes")?;

		let analyzer = IndexAnalyzer::new(indexes, cfg.with.as_ref());
		let candidates = analyzer.analyze(cfg.cond.as_ref(), cfg.order.as_ref());
		Some(select_access_path(candidates, cfg.with.as_ref(), cfg.direction))
	};

	match access_path {
		// B-tree index scan (single-column only)
		Some(AccessPath::BTreeScan {
			index_ref,
			access,
			direction,
		}) if index_ref.cols.len() == 1 => {
			let operator = IndexScan::new(index_ref, access, direction, cfg.table_name);
			let stream = operator.execute(ctx)?;
			Ok((stream, 0))
		}

		// Full-text search
		Some(AccessPath::FullTextSearch {
			index_ref,
			query,
			operator,
		}) => {
			let ft_op = FullTextScan::new(index_ref, query, operator, cfg.table_name);
			let stream = ft_op.execute(ctx)?;
			Ok((stream, 0))
		}

		// Fall back to table KV scan (NOINDEX, compound indexes, KNN, etc.)
		_ => {
			let beg = record::prefix(cfg.ns_id, cfg.db_id, &cfg.table_name)?;
			let end = record::suffix(cfg.ns_id, cfg.db_id, &cfg.table_name)?;
			let stream = kv_scan_stream(
				txn,
				beg,
				end,
				cfg.version,
				cfg.storage_limit,
				cfg.direction,
				cfg.pre_skip,
			);
			Ok((stream, cfg.pre_skip))
		}
	}
}

// ---------------------------------------------------------------------------
// Batch processing
// ---------------------------------------------------------------------------

/// Combined single-pass filter and process for a batch of decoded values.
///
/// Per-record pipeline (sequential, in-place):
///   table permission -> computed fields -> WHERE predicate -> field permissions.
/// Records that fail any check are compacted out via an in-place swap so the
/// surviving prefix can be truncated at the end with no extra allocation.
async fn filter_and_process_batch(
	batch: &mut Vec<Value>,
	permission: &PhysicalPermission,
	predicate: Option<&Arc<dyn PhysicalExpr>>,
	ctx: &ExecutionContext,
	state: &FieldState,
	check_perms: bool,
) -> Result<(), ControlFlow> {
	let needs_perm_filter = !matches!(permission, PhysicalPermission::Allow);
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

// ---------------------------------------------------------------------------
// Key helpers
// ---------------------------------------------------------------------------

/// Compute the start key for a range scan.
fn range_start_key(
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
fn range_end_key(
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
async fn eval_limit_expr(
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

/// Decode a record from its key and value bytes.
#[inline]
fn decode_record(key: &[u8], val: Vec<u8>) -> Result<Value, ControlFlow> {
	let decoded_key =
		crate::key::record::RecordKey::decode_key(key).context("Failed to decode record key")?;

	let rid = crate::val::RecordId {
		table: decoded_key.tb.into_owned(),
		key: decoded_key.id,
	};

	let mut record =
		crate::catalog::Record::kv_decode_value(val).context("Failed to deserialize record")?;

	// Inject the id field into the document
	record.data.to_mut().def(&rid);

	// Take ownership of the value (zero-cost move for freshly deserialized Mutable data)
	Ok(record.data.into_value())
}

// ---------------------------------------------------------------------------
// Field state
// ---------------------------------------------------------------------------

/// Cached state for field processing (computed fields and permissions).
/// Initialized on first batch and reused for subsequent batches.
#[derive(Debug)]
struct FieldState {
	/// Computed field definitions converted to physical expressions
	computed_fields: Vec<ComputedFieldDef>,
	/// Field-level permissions (field name -> permission)
	field_permissions: HashMap<String, PhysicalPermission>,
}

/// A computed field definition ready for evaluation.
#[derive(Debug)]
struct ComputedFieldDef {
	/// The field name where to store the result
	field_name: String,
	/// The physical expression to evaluate
	expr: Arc<dyn PhysicalExpr>,
	/// Optional type coercion
	kind: Option<crate::expr::Kind>,
}

/// Fetch field definitions and build the cached field state.
#[allow(clippy::type_complexity)]
async fn build_field_state(
	ctx: &ExecutionContext,
	table_name: &TableName,
	check_perms: bool,
	needed_fields: Option<&std::collections::HashSet<String>>,
) -> Result<FieldState, ControlFlow> {
	let db_ctx = ctx.database().context("build_field_state requires database context")?;
	let txn = ctx.txn();

	let field_defs = txn
		.all_tb_fields(db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id, table_name, None)
		.await
		.context("Failed to get field definitions")?;

	// Collect computed fields and their dependency metadata
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

			// Get deps: use stored deps, or extract on the fly for legacy fields
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
				expr_to_physical_expr(expr.clone(), ctx.ctx()).with_context(|| {
					format!("Computed field '{field_name}' has unsupported expression")
				})?;

			raw_computed.push((field_name, physical_expr, fd.field_kind.clone(), deps.fields));
		}
	}

	// Determine which computed fields are actually needed
	let needed_computed = if let Some(needed) = needed_fields {
		crate::expr::computed_deps::resolve_required_computed_fields(needed, &dep_map)
	} else {
		// SELECT * -- compute all
		None
	};

	// Filter to only needed computed fields
	let filtered: Vec<_> = if let Some(ref required) = needed_computed {
		raw_computed.into_iter().filter(|(name, _, _, _)| required.contains(name)).collect()
	} else {
		raw_computed
	};

	// Topologically sort for correct evaluation order
	let topo_input: Vec<(String, Vec<String>)> =
		filtered.iter().map(|(name, _, _, deps)| (name.clone(), deps.clone())).collect();
	let sorted_indices = crate::expr::computed_deps::topological_sort_computed_fields(&topo_input);

	let mut computed_fields = Vec::with_capacity(sorted_indices.len());
	for idx in sorted_indices {
		let (field_name, expr, kind, _) = &filtered[idx];
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
			let physical_perm = convert_permission_to_physical(&fd.select_permission, ctx.ctx())
				.context("Failed to convert field permission")?;
			field_permissions.insert(field_name, physical_perm);
		}
	}

	Ok(FieldState {
		computed_fields,
		field_permissions,
	})
}

/// Compute all computed fields for a single value.
async fn compute_fields_for_value(
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
async fn filter_fields_by_permission(
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

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ctx::Context;
	use crate::exec::planner::expr_to_physical_expr;

	/// Helper to create a Scan with all fields for testing
	fn create_test_scan(table_name: &str, with_index_hints: bool) -> Scan {
		let ctx = std::sync::Arc::new(Context::background());
		let source = expr_to_physical_expr(
			crate::expr::Expr::Literal(crate::expr::literal::Literal::String(
				table_name.to_string(),
			)),
			&ctx,
		)
		.expect("Failed to create physical expression");

		Scan::new(
			source,
			None,
			None,
			None,
			if with_index_hints {
				Some(With::NoIndex)
			} else {
				None
			},
			None,
			None,
			None,
			None,
		)
	}

	#[test]
	fn test_scan_struct_with_index_fields() {
		// Test that Scan can be created with all fields
		let scan = create_test_scan("test_table", false);
		assert!(scan.cond.is_none());
		assert!(scan.order.is_none());
		assert!(scan.with.is_none());
	}

	#[test]
	fn test_scan_struct_with_noindex_hint() {
		// Test that Scan can be created with WITH NOINDEX
		let scan = create_test_scan("test_table", true);
		assert!(scan.with.is_some());
		assert!(matches!(scan.with, Some(With::NoIndex)));
	}

	#[test]
	fn test_scan_operator_name() {
		let scan = create_test_scan("test_table", false);
		assert_eq!(scan.name(), "Scan");
	}

	#[test]
	fn test_scan_required_context() {
		let scan = create_test_scan("test_table", false);
		assert!(matches!(scan.required_context(), ContextLevel::Database));
	}

	#[test]
	fn test_determine_scan_direction_no_order() {
		// No order -> Forward
		let direction = determine_scan_direction(&None);
		assert!(matches!(direction, ScanDirection::Forward));
	}

	#[test]
	fn test_determine_scan_direction_random_order() {
		use crate::expr::order::Ordering;

		// Random order -> Forward
		let order = Ordering::Random;
		let direction = determine_scan_direction(&Some(order));
		assert!(matches!(direction, ScanDirection::Forward));
	}
}
