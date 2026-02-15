//! TableScan operator — direct KV range scan over a known table.
//!
//! Created by the planner when the access path is resolved to a table scan
//! at plan time. Skips runtime index analysis and source expression evaluation,
//! going straight to `kv_scan_stream` + `ScanPipeline`.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use tracing::instrument;

use super::pipeline::{ScanPipeline, build_field_state, eval_limit_expr, kv_scan_stream};
use crate::catalog::providers::TableProvider;
use crate::exec::permission::{
	PhysicalPermission, convert_permission_to_physical, should_check_perms,
	validate_record_user_access,
};
use crate::exec::{
	AccessMode, ContextLevel, ExecOperator, ExecutionContext, FlowResult, OperatorMetrics,
	OutputOrdering, PhysicalExpr, ValueBatch, ValueBatchStream, monitor_stream,
};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::iam::Action;
use crate::idx::planner::ScanDirection;
use crate::key::record;
use crate::val::TableName;

/// Direct KV range scan over a known table.
///
/// Unlike [`DynamicScan`](super::DynamicScan), this operator knows at plan
/// time that it's scanning a specific table with no index. It skips:
/// - Source expression evaluation (table name is known)
/// - `IndexAnalyzer` dispatch (access path is `TableScan`)
///
/// It reuses `ScanPipeline` for predicate pushdown, computed fields,
/// permissions, and limit/start handling.
#[derive(Debug, Clone)]
pub struct TableScan {
	pub(crate) table_name: TableName,
	pub(crate) direction: ScanDirection,
	pub(crate) version: Option<Arc<dyn PhysicalExpr>>,
	pub(crate) predicate: Option<Arc<dyn PhysicalExpr>>,
	pub(crate) limit: Option<Arc<dyn PhysicalExpr>>,
	pub(crate) start: Option<Arc<dyn PhysicalExpr>>,
	pub(crate) needed_fields: Option<std::collections::HashSet<String>>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl TableScan {
	pub(crate) fn new(
		table_name: TableName,
		direction: ScanDirection,
		version: Option<Arc<dyn PhysicalExpr>>,
		predicate: Option<Arc<dyn PhysicalExpr>>,
		limit: Option<Arc<dyn PhysicalExpr>>,
		start: Option<Arc<dyn PhysicalExpr>>,
		needed_fields: Option<std::collections::HashSet<String>>,
	) -> Self {
		Self {
			table_name,
			direction,
			version,
			predicate,
			limit,
			start,
			needed_fields,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for TableScan {
	fn name(&self) -> &'static str {
		"TableScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![("table".to_string(), self.table_name.to_string())];
		attrs.push(("direction".to_string(), format!("{:?}", self.direction)));
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

	fn access_mode(&self) -> AccessMode {
		let mut mode = AccessMode::ReadOnly;
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

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn output_ordering(&self) -> OutputOrdering {
		use crate::exec::operators::SortDirection;
		use crate::exec::ordering::SortProperty;

		let dir = match self.direction {
			ScanDirection::Forward => SortDirection::Asc,
			ScanDirection::Backward => SortDirection::Desc,
		};
		OutputOrdering::Sorted(vec![SortProperty {
			path: crate::exec::field_path::FieldPath::field("id"),
			direction: dir,
			collate: false,
			numeric: false,
		}])
	}

	#[instrument(name = "TableScan::execute", level = "trace", skip_all)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();

		// Validate record user has access to this namespace/database
		validate_record_user_access(&db_ctx)?;

		// Check if we need to enforce permissions
		let check_perms = should_check_perms(&db_ctx, Action::View)?;

		// Clone for the async block
		let table_name = self.table_name.clone();
		let direction = self.direction;
		let version_expr = self.version.clone();
		let predicate = self.predicate.clone();
		let limit_expr = self.limit.clone();
		let start_expr = self.start.clone();
		let needed_fields = self.needed_fields.clone();
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			let db_ctx = ctx.database().context("TableScan requires database context")?;
			let txn = ctx.txn();
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);

			// Evaluate pushed-down LIMIT and START expressions
			let limit_val: Option<usize> = match &limit_expr {
				Some(expr) => Some(eval_limit_expr(&**expr, &ctx).await?),
				None => None,
			};
			let start_val: usize = match &start_expr {
				Some(expr) => eval_limit_expr(&**expr, &ctx).await?,
				None => 0,
			};

			// Evaluate VERSION expression
			let version: Option<u64> = match &version_expr {
				Some(expr) => {
					let eval_ctx = crate::exec::EvalContext::from_exec_ctx(&ctx);
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
				Err(ControlFlow::Err(anyhow::Error::new(crate::err::Error::TbNotFound {
					name: table_name.clone(),
				})))?;
			}

			let select_permission = if check_perms {
				let catalog_perm = match &table_def {
					Some(def) => def.permissions.select.clone(),
					None => crate::catalog::Permission::None,
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

			// Build field state (computed fields + field permissions)
			let field_state = build_field_state(
				&ctx, &table_name, check_perms, needed_fields.as_ref(),
			).await?;

			// Pre-compute whether any post-decode processing is needed
			let needs_processing = !matches!(select_permission, PhysicalPermission::Allow)
				|| !field_state.computed_fields.is_empty()
				|| (check_perms && !field_state.field_permissions.is_empty())
				|| predicate.is_some();

			// When no processing is needed, push start/limit to the KV layer
			let pre_skip = if !needs_processing { start_val } else { 0 };
			let effective_storage_limit = if !needs_processing { limit_val } else { None };

			// Create KV range scan stream
			let beg = record::prefix(ns.namespace_id, db.database_id, &table_name)?;
			let end = record::suffix(ns.namespace_id, db.database_id, &table_name)?;
			// Enable prefetching and larger initial batch for full scans (no limit pushed)
			let prefetch = effective_storage_limit.is_none();
			let mut source = kv_scan_stream(
				Arc::clone(&txn), beg, end, version,
				effective_storage_limit, direction, pre_skip, prefetch,
			);

			// Build the pipeline
			let mut pipeline = ScanPipeline::new(
				select_permission, predicate, field_state,
				check_perms, limit_val, start_val.saturating_sub(pre_skip),
			);

			// Consume the stream
			while let Some(batch_result) = source.next().await {
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

		Ok(monitor_stream(Box::pin(stream), "TableScan", &self.metrics))
	}
}
