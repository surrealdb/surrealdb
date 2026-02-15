use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use tracing::instrument;

use super::pipeline::{
	build_field_state, determine_scan_direction, eval_limit_expr, kv_scan_stream,
};
use super::{FullTextScan, IndexScan, KnnScan};
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId, Permission};
use crate::err::Error;
use crate::exec::index::access_path::{AccessPath, select_access_path};
use crate::exec::index::analysis::IndexAnalyzer;
use crate::exec::operators::scan::pipeline::ScanPipeline;
use crate::exec::permission::{
	PhysicalPermission, convert_permission_to_physical, should_check_perms,
	validate_record_user_access,
};
use crate::exec::planner::util::strip_knn_from_condition;
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
use crate::val::{TableName, Value};

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
pub struct DynamicScan {
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
	/// KNN distance context, shared with IndexFunctionExec for vector::distance::knn().
	/// Populated by KnnScan during execution.
	pub(crate) knn_context: Option<Arc<crate::exec::function::KnnContext>>,
}

impl DynamicScan {
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
			knn_context: None,
		}
	}

	/// Set the KNN context for distance propagation.
	pub(crate) fn with_knn_context(
		mut self,
		knn_context: Option<Arc<crate::exec::function::KnnContext>>,
	) -> Self {
		self.knn_context = knn_context;
		self
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for DynamicScan {
	fn name(&self) -> &'static str {
		"DynamicScan"
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
		// Scan needs database context for table access, combined with expression contexts
		let exprs_ctx = [
			Some(&self.source),
			self.version.as_ref(),
			self.predicate.as_ref(),
			self.limit.as_ref(),
			self.start.as_ref(),
		]
		.into_iter()
		.flatten()
		.map(|e| e.required_context())
		.max()
		.unwrap_or(ContextLevel::Root);
		exprs_ctx.max(ContextLevel::Database)
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
		// Scan is read-only, but expressions could contain subqueries with mutations.
		let mut mode = self.source.access_mode();
		if let Some(ref version) = self.version {
			mode = mode.combine(version.access_mode());
		}
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
		let knn_context = self.knn_context.clone();
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
			let table_name = match table_value {
				Value::Table(t) => t,
			Value::RecordId(rid) => {
				// === RECORD LOOKUP (point or range) ===
				// Delegate to the shared execute_record_lookup helper which
				// handles both point lookups and range scans. For plan-time-
				// known RecordIds the planner emits RecordLookup directly;
				// this path handles runtime-discovered RecordIds (e.g. from
				// `type::thing(...)` or other dynamic expressions).
				//
				// The planner marks predicate/limit/start as consumed for
				// FunctionCall/Postfix sources, so the outer Filter/Limit
				// operators are removed. We must forward the pushdowns here
				// to ensure WHERE/LIMIT/START are still applied.

				// Evaluate VERSION expression
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

				// Evaluate pushed-down LIMIT and START expressions
				let limit_val: Option<usize> = match &limit_expr {
					Some(expr) => Some(eval_limit_expr(&**expr, &ctx).await?),
					None => None,
				};
				let start_val: usize = match &start_expr {
					Some(expr) => eval_limit_expr(&**expr, &ctx).await?,
					None => 0,
				};

				// Early exit if limit is 0
				if limit_val == Some(0) {
					return;
				}

				let results = super::record_id::execute_record_lookup(
					&rid, version, check_perms, needed_fields.as_ref(), &ctx,
					predicate.as_ref(), limit_val, start_val,
				).await?;

				if !results.is_empty() {
					yield ValueBatch { values: results };
				}
				return;
			}
			Value::Array(arr) => {
				// === ARRAY SOURCE ===
				// The planner marks predicate/limit/start as consumed for
				// FunctionCall/Postfix sources, so the outer Filter/Limit
				// operators are removed. We must apply them here.

				// Evaluate pushed-down LIMIT and START expressions
				let limit_val: Option<usize> = match &limit_expr {
					Some(expr) => Some(eval_limit_expr(&**expr, &ctx).await?),
					None => None,
				};
				let start_val: usize = match &start_expr {
					Some(expr) => eval_limit_expr(&**expr, &ctx).await?,
					None => 0,
				};

				// Early exit if limit is 0
				if limit_val == Some(0) {
					return;
				}

				// Apply pushed-down predicate
				let mut values = arr.0;
				if let Some(ref pred) = predicate {
					let mut write_idx = 0;
					for read_idx in 0..values.len() {
						let eval_ctx = EvalContext::from_exec_ctx(&ctx).with_value(&values[read_idx]);
						if pred.evaluate(eval_ctx).await?.is_truthy() {
							if write_idx != read_idx {
								values.swap(write_idx, read_idx);
							}
							write_idx += 1;
						}
					}
					values.truncate(write_idx);
				}

				// Apply start offset
				if start_val > 0 {
					if start_val >= values.len() {
						return;
					}
					values.drain(..start_val);
				}

				// Apply limit
				if let Some(limit) = limit_val {
					values.truncate(limit);
				}

				if !values.is_empty() {
					yield ValueBatch { values };
				}
				return;
			}
			// For any other value type, yield as a single row.
			// This matches legacy FROM behavior for non-table values.
			other => {
				// === SCALAR SOURCE ===
				// Same pushdown logic as the array branch above.

				// Evaluate pushed-down LIMIT and START expressions
				let limit_val: Option<usize> = match &limit_expr {
					Some(expr) => Some(eval_limit_expr(&**expr, &ctx).await?),
					None => None,
				};
				let start_val: usize = match &start_expr {
					Some(expr) => eval_limit_expr(&**expr, &ctx).await?,
					None => 0,
				};

				// Early exit if limit is 0 or start skips past the single value
				if limit_val == Some(0) || start_val > 0 {
					return;
				}

				// Apply pushed-down predicate
				if let Some(ref pred) = predicate {
					let eval_ctx = EvalContext::from_exec_ctx(&ctx).with_value(&other);
					if !pred.evaluate(eval_ctx).await?.is_truthy() {
						return;
					}
				}

				yield ValueBatch { values: vec![other] };
				return;
			}
			};

			// === TABLE SCAN PATH ===
			// Everything below is for table-based scans only.

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
				convert_permission_to_physical(&catalog_perm, ctx.ctx()).await
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

			// When no processing is needed, push start to the KV layer as pre_skip
			// so rows are discarded before deserialization.
			let pre_skip = if !needs_processing { start_val } else { 0 };

			// Push limit to the storage layer for the pure fast path.
			// The KV scanner's skip is applied before the limit, so no adjustment needed.
			let effective_storage_limit = if !needs_processing { limit_val } else { None };

			let direction = determine_scan_direction(&order);

			// Create the source stream based on scan type.
			// `applied_pre_skip` tracks how many rows the source will skip
			// before decoding, so the pipeline can adjust its start accordingly.
			let (mut source, applied_pre_skip) = {
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
						knn_context: knn_context.clone(),
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
// Source helpers
// ---------------------------------------------------------------------------

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
	/// KNN distance context for vector::distance::knn() support.
	knn_context: Option<Arc<crate::exec::function::KnnContext>>,
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
		if candidates.is_empty() {
			// No single-index candidates -- try multi-index union for OR conditions
			analyzer
				.try_or_union(cfg.cond.as_ref(), cfg.direction)
				// Try expanding IN operators into union of equality lookups
				.or_else(|| analyzer.try_in_expansion(cfg.cond.as_ref(), cfg.direction))
		} else {
			let path = select_access_path(candidates, cfg.with.as_ref(), cfg.direction);
			// When the best single-index path is a full-range scan (ORDER BY
			// only), prefer a multi-index union for OR conditions if available.
			if path.is_full_range_scan() {
				analyzer.try_or_union(cfg.cond.as_ref(), cfg.direction).or(Some(path))
			} else {
				Some(path)
			}
		}
	};

	match access_path {
		// B-tree index scan (single-column and compound)
		Some(AccessPath::BTreeScan {
			index_ref,
			access,
			direction,
		}) => {
			let operator = IndexScan::new(index_ref, access, direction, cfg.table_name, None, None);
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

		// KNN vector search via HNSW index
		Some(AccessPath::KnnSearch {
			index_ref,
			vector,
			k,
			ef,
		}) => {
			// Strip KNN operators from the condition to get the residual
			// (non-KNN predicates) for HNSW pushdown.
			let residual_cond = cfg.cond.as_ref().and_then(strip_knn_from_condition);
			let knn_op = KnnScan::new(
				index_ref,
				vector,
				k,
				ef,
				cfg.table_name,
				cfg.knn_context.clone(),
				residual_cond,
			);
			let stream = knn_op.execute(ctx)?;
			Ok((stream, 0))
		}

		// Multi-index union for OR conditions — delegate to UnionIndexScan
		Some(AccessPath::Union(paths)) => {
			let mut sub_operators: Vec<Arc<dyn ExecOperator>> = Vec::with_capacity(paths.len());
			for path in paths {
				sub_operators.push(create_index_operator(&path, &cfg));
			}
			let union_op = super::UnionIndexScan::new(sub_operators);
			let stream = union_op.execute(ctx)?;
			Ok((stream, 0))
		}

		// Fall back to table KV scan (NOINDEX, etc.)
		_ => {
			let beg = record::prefix(cfg.ns_id, cfg.db_id, &cfg.table_name)?;
			let end = record::suffix(cfg.ns_id, cfg.db_id, &cfg.table_name)?;
			// Enable prefetching for full scans (no limit pushed)
			let prefetch = cfg.storage_limit.is_none();
			let stream = kv_scan_stream(
				txn,
				beg,
				end,
				cfg.version,
				cfg.storage_limit,
				cfg.direction,
				cfg.pre_skip,
				prefetch,
			);
			Ok((stream, cfg.pre_skip))
		}
	}
}

/// Create an index scan operator for a single access path.
///
/// Used by the multi-index union handler to create individual operators
/// for each branch of an OR condition. The caller (typically
/// [`UnionIndexScan`](super::UnionIndexScan)) is responsible for
/// executing the operators and deduplicating results.
fn create_index_operator(path: &AccessPath, cfg: &TableScanConfig) -> Arc<dyn ExecOperator> {
	match path {
		AccessPath::BTreeScan {
			index_ref,
			access,
			direction,
		} => Arc::new(IndexScan::new(
			index_ref.clone(),
			access.clone(),
			*direction,
			cfg.table_name.clone(),
			None,
			None,
		)),
		AccessPath::FullTextSearch {
			index_ref,
			query,
			operator,
		} => Arc::new(FullTextScan::new(
			index_ref.clone(),
			query.clone(),
			operator.clone(),
			cfg.table_name.clone(),
		)),
		AccessPath::KnnSearch {
			index_ref,
			vector,
			k,
			ef,
		} => {
			let residual_cond = cfg.cond.as_ref().and_then(strip_knn_from_condition);
			Arc::new(KnnScan::new(
				index_ref.clone(),
				vector.clone(),
				*k,
				*ef,
				cfg.table_name.clone(),
				cfg.knn_context.clone(),
				residual_cond,
			))
		}
		// TableScan and nested Union should not appear as sub-paths.
		// Fall back to a table scan operator which will produce all
		// records (safe but sub-optimal).
		AccessPath::TableScan | AccessPath::Union(_) => Arc::new(super::TableScan::new(
			cfg.table_name.clone(),
			cfg.direction,
			None,
			None,
			None,
			None,
			None,
		)),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ctx::Context;
	use crate::exec::planner::expr_to_physical_expr;

	/// Helper to create a Scan with all fields for testing
	async fn create_test_scan(table_name: &str, with_index_hints: bool) -> DynamicScan {
		let ctx = std::sync::Arc::new(Context::background());
		let source = expr_to_physical_expr(
			crate::expr::Expr::Literal(crate::expr::literal::Literal::String(
				table_name.to_string(),
			)),
			&ctx,
		)
		.await
		.expect("Failed to create physical expression");

		DynamicScan::new(
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

	#[tokio::test]
	async fn test_scan_struct_with_index_fields() {
		// Test that Scan can be created with all fields
		let scan = create_test_scan("test_table", false).await;
		assert!(scan.cond.is_none());
		assert!(scan.order.is_none());
		assert!(scan.with.is_none());
	}

	#[tokio::test]
	async fn test_scan_struct_with_noindex_hint() {
		// Test that Scan can be created with WITH NOINDEX
		let scan = create_test_scan("test_table", true).await;
		assert!(scan.with.is_some());
		assert!(matches!(scan.with, Some(With::NoIndex)));
	}

	#[tokio::test]
	async fn test_scan_operator_name() {
		let scan = create_test_scan("test_table", false).await;
		assert_eq!(scan.name(), "DynamicScan");
	}

	#[tokio::test]
	async fn test_scan_required_context() {
		let scan = create_test_scan("test_table", false).await;
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
