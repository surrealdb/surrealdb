//! TableScan operator — direct KV range scan over a known table.
//!
//! Created by the planner when the access path is resolved to a table scan
//! at plan time. Skips runtime index analysis and source expression evaluation,
//! going straight to `kv_scan_stream` + `ScanPipeline`.

#[cfg(test)]
use std::borrow::Cow;
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use common::future::stream::Yielder;
#[cfg(test)]
use futures::StreamExt;
#[cfg(test)]
use surrealdb_strand::TableName;

#[cfg(test)]
#[cfg(test)]
use super::pipeline::eval_limit_expr;
#[cfg(test)]
use super::pipeline::{FieldState, ScanPipeline, build_field_state, kv_scan_stream};
#[cfg(test)]
use super::resolved::{EnforcementMode, ResolvedTableContext, planned_select_permission};
#[cfg(test)]
use crate::err::EngineError;
#[cfg(test)]
use crate::exec::operators::limit::PagingClause;
#[cfg(test)]
use crate::exec::operators::scan::common::resolve_version_stamp;
#[cfg(test)]
use crate::exec::permission::{
	PhysicalPermission, should_check_perms, table_select_permission_physical_runtime,
	validate_record_user_access,
};
#[cfg(test)]
use crate::exec::pre_decode_filter::{PreDecodeFilterStatus, pre_decode_filter_for_execute};
#[cfg(test)]
use crate::exec::topk_pushdown::{TopKPushdownStatus, topk_probe_for_execute};
#[cfg(test)]
use crate::exec::{
	AccessMode, ContextLevel, ExecOperator, ExecutionContext, FlowResult, OperatorMetrics,
	OutputOrdering, PhysicalExpr, ValueBatchStream, monitor_stream,
};
#[cfg(test)]
use crate::expr::{ControlFlow, ControlFlowExt};
#[cfg(test)]
use crate::iam::Action;
#[cfg(test)]
use crate::key::schema::RecordPrefix;
#[cfg(test)]
use crate::kvs::Direction;

#[cfg(test)]
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
	pub(crate) direction: Direction,
	pub(crate) version: Option<Arc<dyn PhysicalExpr>>,
	pub(crate) predicate: Option<Arc<dyn PhysicalExpr>>,
	pub(crate) limit: Option<Arc<dyn PhysicalExpr>>,
	pub(crate) start: Option<Arc<dyn PhysicalExpr>>,
	pub(crate) needed_fields: Option<std::collections::HashSet<String>>,
	/// Plan-time resolved table context. When present, `execute()` skips
	/// all runtime metadata lookups (table def, permissions, field state), and
	/// its enforcement mode says whether the scan applies the table
	/// permission, computed fields and field permissions itself or yields rows
	/// as stored for the plan above it to apply them.
	pub(crate) resolved: Option<ResolvedTableContext>,
	/// The mode the scan was planned under. A resolved context agrees with it;
	/// without one the scan resolves its table when it runs and, under
	/// [`EnforcementMode::Planned`], still yields rows as stored: the table was
	/// not in the catalog when the plan was made, and the plan above the scan
	/// was built from that absence — a permission-checked caller is denied
	/// there — so nothing is applied here.
	pub(crate) mode: EnforcementMode,
	/// Predicate pre-decode filter status (plan-time); see [`PreDecodeFilterStatus`].
	pub(crate) pre_decode_filter_status: PreDecodeFilterStatus,
	/// TopK threshold pushdown status (plan-time); see [`TopKPushdownStatus`].
	pub(crate) topk_pushdown_status: TopKPushdownStatus,
	pub(crate) metrics: Arc<OperatorMetrics>,
	/// The limit this scan is bound to, which spares it its records when it
	/// evaluates to zero.
	pub(crate) zero_limit: crate::exec::zero_limit::ZeroLimitBinding,
}

#[cfg(test)]
impl TableScan {
	#[cfg(test)]
	pub(crate) fn new(
		table_name: TableName,
		direction: Direction,
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
			resolved: None,
			mode: EnforcementMode::Runtime,
			pre_decode_filter_status: PreDecodeFilterStatus::NotApplicable,
			topk_pushdown_status: TopKPushdownStatus::NotApplicable,
			zero_limit: Default::default(),
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	#[cfg(test)]
	/// Set the plan-time resolved table context.
	pub(crate) fn with_resolved(mut self, resolved: ResolvedTableContext) -> Self {
		self.resolved = Some(resolved);
		self
	}

	#[cfg(test)]
	/// Set the mode the scan was planned under; see [`Self::mode`].
	pub(crate) fn with_enforcement_mode(mut self, mode: EnforcementMode) -> Self {
		self.mode = mode;
		self
	}

	#[cfg(test)]
	/// Set plan-time pre-decode filter status for EXPLAIN and execution.
	pub(crate) fn with_pre_decode_filter(mut self, status: PreDecodeFilterStatus) -> Self {
		self.pre_decode_filter_status = status;
		self
	}

	#[cfg(test)]
	/// Set plan-time TopK threshold pushdown status for EXPLAIN and execution.
	pub(crate) fn with_topk_pushdown(mut self, status: TopKPushdownStatus) -> Self {
		self.topk_pushdown_status = status;
		self
	}
}
#[cfg(test)]
impl ExecOperator for TableScan {
	fn name(&self) -> &'static str {
		"TableScan"
	}

	fn zero_limit_binding(&self) -> Option<&crate::exec::zero_limit::ZeroLimitBinding> {
		Some(&self.zero_limit)
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
		if let Some(s) = self.pre_decode_filter_status.explain_text() {
			attrs.push(("pre_decode_filter".to_string(), s.to_string()));
		}
		if let Some(s) = self.topk_pushdown_status.explain_text() {
			attrs.push(("topk_pushdown".to_string(), s.to_string()));
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
			Direction::Forward => SortDirection::Asc,
			Direction::Backward => SortDirection::Desc,
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
		validate_record_user_access(&db_ctx)?;
		let check_perms = should_check_perms(&db_ctx, Action::View)?;

		let resolved = self.resolved.clone();
		let mode = self.mode;
		let table_name = self.table_name.clone();
		let direction = self.direction;
		let version_expr = self.version.clone();
		let predicate = self.predicate.clone();
		let limit_expr = self.limit.clone();
		let start_expr = self.start.clone();
		let needed_fields = self.needed_fields.clone();
		let pre_decode_filter_status = self.pre_decode_filter_status.clone();
		let topk_pushdown_status = self.topk_pushdown_status.clone();
		let metrics = Arc::clone(&self.metrics);
		let zeroed = self.zero_limit.applies(ctx);
		let ctx = ctx.clone();

		let stream =
			common::future::stream::try_async_stream(async move |mut yielder: Yielder<_>| {
				let db_ctx = ctx.database().context("TableScan requires database context")?;
				let txn = ctx.txn();
				let ns = Arc::clone(&db_ctx.ns_ctx.ns);
				let db = Arc::clone(&db_ctx.db);

				// Evaluate pushed-down LIMIT and START expressions
				let limit_val: Option<usize> = match &limit_expr {
					Some(expr) => Some(eval_limit_expr(&**expr, &ctx, PagingClause::Limit).await?),
					None => None,
				};
				let limit_val = if zeroed {
					Some(0)
				} else {
					limit_val
				};
				let start_val: usize = match &start_expr {
					Some(expr) => eval_limit_expr(&**expr, &ctx, PagingClause::Start).await?,
					None => 0,
				};

				// Resolve VERSION timestamp; see [`resolve_version_stamp`] for
				// why we prefer the stamp already set by the enclosing
				// `VersionScope` over re-evaluating `version_expr` here.
				let version: Option<u64> =
					resolve_version_stamp(&ctx, version_expr.as_ref()).await?;

				// Resolve table metadata: plan-time fast path or runtime fallback
				let (select_permission, field_state) = if let Some(ref res) = resolved {
					// Plan-time resolved: use pre-fetched table def + field state.
					// Only the permission compilation (pure CPU) happens here.
					let perm = res.select_permission(check_perms);
					let fs = res.field_state_for_projection(needed_fields.as_ref());
					(perm, fs)
				} else {
					// Runtime resolution: the table was not in the catalog when
					// the plan was made, or the plan was made without one.
					let Some(table_def) = db_ctx
						.get_table_def(&table_name, version)
						.await
						.context("Failed to get table")?
					else {
						return Err(ControlFlow::Err(anyhow::Error::new(
							crate::catalog::Error::TbNotFound {
								name: table_name.clone(),
							},
						)));
					};

					// Fail closed: a record-range scan over a lightweight
					// relation would be wrong-empty. Statically-planned scans
					// were declined at plan time; only runtime-resolved paths
					// reach this.
					if crate::kvs::lightweight::lightweight_relation(&table_def.table_type)
						.is_some()
					{
						return Err(ControlFlow::Err(anyhow::anyhow!(
							"a LIGHTWEIGHT relation cannot be scanned by this execution path; \
							 reference the table statically so the record-less scan can serve it"
						)));
					}

					match mode {
						// The plan above enforces; see `TableScan::mode`.
						EnforcementMode::Planned => (
							if check_perms {
								planned_select_permission(&table_def)
							} else {
								PhysicalPermission::Allow
							},
							FieldState::empty(),
						),
						EnforcementMode::Runtime => {
							let perm = if check_perms {
								table_select_permission_physical_runtime(Some(&*table_def), &ctx)
									.await
									.context("Failed to convert permission")?
							} else {
								PhysicalPermission::Allow
							};
							let fs = build_field_state(
								&ctx,
								&table_name,
								check_perms,
								needed_fields.as_ref(),
							)
							.await?;
							(perm, fs)
						}
					}
				};

				if matches!(select_permission, PhysicalPermission::Deny) {
					return Ok(());
				}

				if limit_val == Some(0) {
					return Ok(());
				}

				// Row-filtering (permissions, WHERE) prevents positional pushdown;
				// row-modifying ops (computed fields, field perms) do not.
				let needs_row_filtering = ScanPipeline::compute_needs_row_filtering(
					&select_permission,
					predicate.as_ref(),
				);

				let pre_skip = if !needs_row_filtering {
					start_val
				} else {
					0
				};
				let effective_storage_limit = if !needs_row_filtering {
					limit_val
				} else {
					None
				};

				let range = RecordPrefix {
					ns: ns.namespace_id,
					db: db.database_id,
					tb: Cow::Borrowed(&table_name),
				}
				.range()?;
				let limit_hint = limit_val.map(|l| (l + start_val).try_into().unwrap_or(u32::MAX));
				let pre_decode_filter = pre_decode_filter_for_execute(
					&pre_decode_filter_status,
					&field_state,
					check_perms,
					ctx.ctx().config.exec.idiom_recursion_limit,
				);
				let topk_probe = topk_probe_for_execute(
					&topk_pushdown_status,
					&field_state,
					check_perms,
					&metrics,
					&ctx,
				);
				let mut source = kv_scan_stream(
					Arc::clone(&txn),
					range,
					version,
					effective_storage_limit,
					direction,
					pre_skip,
					limit_hint,
					pre_decode_filter,
					topk_probe,
				);

				let mut pipeline = ScanPipeline::new(
					select_permission,
					predicate,
					field_state,
					check_perms,
					limit_val,
					start_val.saturating_sub(pre_skip),
				);

				while let Some(batch_result) = source.next().await {
					if ctx.cancellation().is_cancelled() {
						Err(ControlFlow::Err(anyhow::anyhow!(EngineError::QueryCancelled)))?;
					}
					let mut batch = batch_result?;
					let cont = pipeline.process_batch(batch.values_mut(), &ctx).await?;
					if !batch.is_empty() {
						yielder.emit(batch).await;
					}
					if !cont {
						break;
					}
				}
				Ok(())
			});

		Ok(monitor_stream(Box::pin(stream), "TableScan", &self.metrics))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::scan::resolved::test_support::{
		RESTRICTED_TABLE, anonymous, assert_as_stored, assert_reduced, contexts,
	};
	use crate::exec::operators::test_util::{TestDb, collect};
	use crate::kvs::TransactionType;

	/// Under the plan's enforcement the scan yields the record as stored, for
	/// the nodes above it to reduce; under its own it reduces the record for a
	/// permission-checked caller.
	#[tokio::test]
	async fn a_planned_scan_yields_the_stored_record_and_a_runtime_scan_reduces_it() {
		let db = TestDb::new_with_auth(RESTRICTED_TABLE).await;
		let ctx = db.exec_ctx_as(&anonymous(), TransactionType::Read).await;
		let (planned, runtime) = contexts(&ctx, "t").await;
		let scan = |tc: ResolvedTableContext| -> Arc<dyn ExecOperator> {
			Arc::new(
				TableScan::new("t".into(), Direction::Forward, None, None, None, None, None)
					.with_resolved(tc),
			)
		};
		assert_as_stored(&collect(&scan(planned), &ctx).await);
		assert_reduced(&collect(&scan(runtime), &ctx).await);
	}

	/// Without a plan-time context the scan resolves its table when it runs,
	/// and the mode it was planned under still decides who enforces.
	#[tokio::test]
	async fn a_scan_resolving_its_table_at_runtime_enforces_by_its_planned_mode() {
		let db = TestDb::new_with_auth(RESTRICTED_TABLE).await;
		let ctx = db.exec_ctx_as(&anonymous(), TransactionType::Read).await;
		let scan = |mode: EnforcementMode| -> Arc<dyn ExecOperator> {
			Arc::new(
				TableScan::new("t".into(), Direction::Forward, None, None, None, None, None)
					.with_enforcement_mode(mode),
			)
		};
		assert_as_stored(&collect(&scan(EnforcementMode::Planned), &ctx).await);
		assert_reduced(&collect(&scan(EnforcementMode::Runtime), &ctx).await);
	}
}
