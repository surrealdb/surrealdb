//! RecordLookup operator — record lookup and range scan by RecordId.
//!
//! Handles both single-record point lookups (`person:1`) and RecordId range
//! scans (`person:1..5`). Created by the planner when the FROM source is a
//! known RecordId (literal or parameter), and also used internally by
//! `DynamicScan` when it discovers a RecordId at runtime.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use tracing::instrument;

use super::pipeline::{
	ScanPipeline, build_field_state, filter_and_process_batch, kv_scan_stream, range_end_key,
	range_start_key,
};
use crate::catalog::providers::TableProvider;
use crate::exec::permission::{
	PhysicalPermission, convert_permission_to_physical, should_check_perms,
	validate_record_user_access,
};
use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, OutputOrdering, PhysicalExpr, ValueBatch, ValueBatchStream, monitor_stream,
};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::iam::Action;
use crate::val::{RecordId, RecordIdKey, Value};

/// Record lookup and range scan by RecordId.
///
/// Handles both single-record point lookups (`person:1`) and RecordId
/// range scans (`person:1..5`). Unlike [`DynamicScan`](super::DynamicScan),
/// this operator knows at plan time that its source is a RecordId. It skips:
/// - Source expression type dispatch (table vs record vs array)
/// - `IndexAnalyzer` / access path selection
///
/// For point lookups, it also skips limit/start tracking and scan pipelines
/// (always 0 or 1 row).
///
/// It reuses `build_field_state` and `filter_and_process_batch` from the
/// shared pipeline infrastructure for permissions and computed fields.
#[derive(Debug, Clone)]
pub struct RecordIdScan {
	/// Expression that evaluates to a RecordId value.
	pub(crate) record_id: Arc<dyn PhysicalExpr>,
	/// Optional VERSION timestamp for time-travel queries.
	pub(crate) version: Option<Arc<dyn PhysicalExpr>>,
	/// Fields needed by the query (projection + WHERE + ORDER + GROUP).
	/// `None` means all fields are needed (SELECT *).
	pub(crate) needed_fields: Option<std::collections::HashSet<String>>,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl RecordIdScan {
	pub(crate) fn new(
		record_id: Arc<dyn PhysicalExpr>,
		version: Option<Arc<dyn PhysicalExpr>>,
		needed_fields: Option<std::collections::HashSet<String>>,
	) -> Self {
		Self {
			record_id,
			version,
			needed_fields,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for RecordIdScan {
	fn name(&self) -> &'static str {
		"RecordIdScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![("record_id".to_string(), self.record_id.to_sql())];
		if let Some(ref version) = self.version {
			attrs.push(("version".to_string(), version.to_sql()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		let mut mode = self.record_id.access_mode();
		if let Some(ref version) = self.version {
			mode = mode.combine(version.access_mode());
		}
		mode
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		let mut exprs = vec![("record_id", &self.record_id)];
		if let Some(ref version) = self.version {
			exprs.push(("version", version));
		}
		exprs
	}

	fn output_ordering(&self) -> OutputOrdering {
		use crate::exec::operators::SortDirection;
		use crate::exec::ordering::SortProperty;

		// Both point lookups (0-1 rows) and range scans (KV-ordered by id)
		// produce output sorted by id ASC.
		OutputOrdering::Sorted(vec![SortProperty {
			path: crate::exec::field_path::FieldPath::field("id"),
			direction: SortDirection::Asc,
		}])
	}

	#[instrument(name = "RecordLookup::execute", level = "trace", skip_all)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();

		// Validate record user has access to this namespace/database
		validate_record_user_access(&db_ctx)?;

		// Check if we need to enforce permissions
		let check_perms = should_check_perms(&db_ctx, Action::View)?;

		// Clone for the async block
		let record_id_expr = Arc::clone(&self.record_id);
		let version_expr = self.version.clone();
		let needed_fields = self.needed_fields.clone();
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			// 1. Evaluate the record_id expression to get the RecordId value
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let rid_value = record_id_expr.evaluate(eval_ctx).await?;

			let rid = match rid_value {
				Value::RecordId(rid) => rid,
				other => {
					// If the expression didn't produce a RecordId, yield as-is
					// (defensive fallback — the planner should only route RecordIds here)
					yield ValueBatch { values: vec![other] };
					return;
				}
			};

			// 2. Evaluate VERSION expression to a timestamp
			let version: Option<u64> = match &version_expr {
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

			// 3. Delegate to the shared lookup helper
			let results = execute_record_lookup(
				&rid, version, check_perms, needed_fields.as_ref(), &ctx,
			).await?;

			if !results.is_empty() {
				yield ValueBatch { values: results };
			}
		};

		Ok(monitor_stream(Box::pin(stream), "RecordLookup", &self.metrics))
	}
}

// =============================================================================
// Shared helper — used by both RecordLookup::execute and DynamicScan
// =============================================================================

/// Execute a record lookup or range scan for a resolved RecordId.
///
/// Handles both point lookups (non-range key) and range scans (range key).
/// Returns the resulting rows after applying permissions and computed fields.
///
/// This is the single implementation of RecordId-based data access, shared
/// by `RecordLookup` (plan-time) and `DynamicScan` (runtime-discovered).
pub(crate) async fn execute_record_lookup(
	rid: &RecordId,
	version: Option<u64>,
	check_perms: bool,
	needed_fields: Option<&std::collections::HashSet<String>>,
	ctx: &ExecutionContext,
) -> Result<Vec<Value>, ControlFlow> {
	let db_ctx = ctx.database().context("RecordLookup requires database context")?;
	let txn = ctx.txn();
	let ns = Arc::clone(&db_ctx.ns_ctx.ns);
	let db = Arc::clone(&db_ctx.db);

	// 1. Check table existence and resolve SELECT permission
	let table_def =
		txn.get_tb_by_name(&ns.name, &db.name, &rid.table).await.context("Failed to get table")?;

	if table_def.is_none() {
		return Err(ControlFlow::Err(anyhow::Error::new(crate::err::Error::TbNotFound {
			name: rid.table.clone(),
		})));
	}

	let select_permission = if check_perms {
		let catalog_perm = match &table_def {
			Some(def) => def.permissions.select.clone(),
			None => crate::catalog::Permission::None,
		};
		convert_permission_to_physical(&catalog_perm, ctx.ctx())
			.await
			.context("Failed to convert permission")?
	} else {
		PhysicalPermission::Allow
	};

	// Early exit if denied
	if matches!(select_permission, PhysicalPermission::Deny) {
		return Ok(vec![]);
	}

	// 2. Build field state (computed fields + field permissions)
	let field_state = build_field_state(ctx, &rid.table, check_perms, needed_fields).await?;

	// Pre-compute whether any post-decode processing is needed
	let needs_processing = !matches!(select_permission, PhysicalPermission::Allow)
		|| !field_state.computed_fields.is_empty()
		|| (check_perms && !field_state.field_permissions.is_empty());

	// 3. Dispatch based on key type
	match &rid.key {
		RecordIdKey::Range(range) => {
			// --- Range scan ---
			let beg = range_start_key(ns.namespace_id, db.database_id, &rid.table, &range.start)?;
			let end = range_end_key(ns.namespace_id, db.database_id, &rid.table, &range.end)?;
			let mut source = kv_scan_stream(
				Arc::clone(&txn),
				beg,
				end,
				version,
				None, // no storage limit for range scans
				crate::idx::planner::ScanDirection::Forward,
				0, // no pre-skip
			);

			let mut pipeline = ScanPipeline::new(
				select_permission,
				None, // no predicate for record lookups
				field_state,
				check_perms,
				None, // no limit
				0,    // no start offset
			);

			let mut results = Vec::new();
			while let Some(batch_result) = source.next().await {
				if ctx.cancellation().is_cancelled() {
					return Err(ControlFlow::Err(anyhow::anyhow!(
						crate::err::Error::QueryCancelled
					)));
				}
				let mut batch = batch_result?;
				let cont = pipeline.process_batch(&mut batch.values, ctx).await?;
				results.extend(batch.values);
				if !cont {
					break;
				}
			}
			Ok(results)
		}
		_ => {
			// --- Point lookup ---
			let record = txn
				.get_record(ns.namespace_id, db.database_id, &rid.table, &rid.key, version)
				.await
				.context("Failed to get record")?;

			if record.data.as_ref().is_none() {
				return Ok(vec![]);
			}

			let mut value = record.data.as_ref().clone();
			value.def(rid);

			let mut batch = vec![value];
			if needs_processing {
				filter_and_process_batch(
					&mut batch,
					&select_permission,
					None, // no predicate
					ctx,
					&field_state,
					check_perms,
				)
				.await?;
			}
			Ok(batch)
		}
	}
}
