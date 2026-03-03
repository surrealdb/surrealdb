//! Union index scan operator for multi-index OR conditions.
//!
//! Created by the planner when the access path is `AccessPath::Union`,
//! meaning the WHERE clause has top-level OR branches that can each be
//! served by a different index. Each sub-operator handles one branch;
//! results are deduplicated by record ID at execution time.
//!
//! Follows the same permission pattern as [`super::TableScan`]: resolves
//! table-level and field-level SELECT permissions, builds computed fields,
//! and applies the full [`ScanPipeline`](super::pipeline::ScanPipeline).

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use tracing::instrument;

use super::pipeline::{ScanPipeline, build_field_state};
use super::resolved::ResolvedTableContext;
use crate::exec::permission::{
	PhysicalPermission, convert_permission_to_physical, should_check_perms,
	validate_record_user_access,
};
use crate::exec::{
	AccessMode, CombineAccessModes, ContextLevel, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::iam::Action;
use crate::val::{RecordId, TableName, Value};

/// Union index scan operator for OR conditions.
///
/// Wraps multiple pre-planned index scan operators (one per OR branch)
/// and executes them sequentially, deduplicating results by record ID
/// so that a record matching multiple branches is only returned once.
///
/// Unlike [`super::super::Union`] (which handles `SELECT FROM a, b, c`),
/// this operator targets a single table with multiple index access paths
/// and performs record-level deduplication.
///
/// Handles table/field-level permissions and computed-field materialization
/// via [`ScanPipeline`], following the same pattern as [`super::TableScan`].
#[derive(Debug)]
pub struct UnionIndexScan {
	pub(crate) table_name: TableName,
	pub(crate) inputs: Vec<Arc<dyn ExecOperator>>,
	pub(crate) needed_fields: Option<HashSet<String>>,
	/// Plan-time resolved table context. When present, `execute()` skips
	/// runtime table def + permission lookup and uses pre-built field state.
	pub(crate) resolved: Option<ResolvedTableContext>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl UnionIndexScan {
	pub(crate) fn new(
		table_name: TableName,
		inputs: Vec<Arc<dyn ExecOperator>>,
		needed_fields: Option<HashSet<String>>,
	) -> Self {
		Self {
			table_name,
			inputs,
			needed_fields,
			resolved: None,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	/// Set the plan-time resolved table context.
	pub(crate) fn with_resolved(mut self, resolved: ResolvedTableContext) -> Self {
		self.resolved = Some(resolved);
		self
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for UnionIndexScan {
	fn name(&self) -> &'static str {
		"UnionIndexScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![
			("table".to_string(), self.table_name.to_string()),
			("branches".to_string(), self.inputs.len().to_string()),
		]
	}

	fn required_context(&self) -> ContextLevel {
		self.inputs
			.iter()
			.map(|input| input.required_context())
			.max()
			.unwrap_or(ContextLevel::Database)
	}

	fn access_mode(&self) -> AccessMode {
		self.inputs.iter().map(|input| input.access_mode()).combine_all()
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		self.inputs.iter().collect()
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	#[instrument(name = "UnionIndexScan::execute", level = "trace", skip_all)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		if self.inputs.is_empty() {
			return Ok(monitor_stream(
				Box::pin(futures::stream::empty()),
				"UnionIndexScan",
				&self.metrics,
			));
		}

		let db_ctx = ctx.database()?.clone();

		// Validate record user has access to this namespace/database
		validate_record_user_access(&db_ctx)?;

		// Check if we need to enforce permissions
		let check_perms = should_check_perms(&db_ctx, Action::View)?;

		// Execute each sub-operator and collect their streams eagerly
		// so that any setup errors surface immediately.
		let mut sub_streams: Vec<ValueBatchStream> = Vec::with_capacity(self.inputs.len());
		for input in &self.inputs {
			let sub_stream = buffer_stream(
				input.execute(ctx)?,
				input.access_mode(),
				input.cardinality_hint(),
				ctx.ctx().config().limits.operator_buffer_size,
			);
			sub_streams.push(sub_stream);
		}

		// Clone for the async block
		let table_name = self.table_name.clone();
		let needed_fields = self.needed_fields.clone();
		let resolved = self.resolved.clone();
		let ctx = ctx.clone();

		let stream: ValueBatchStream = Box::pin(async_stream::try_stream! {
			let db_ctx = ctx.database().context("UnionIndexScan requires database context")?;

			// Resolve table permissions and field state: plan-time fast path or runtime fallback
			let (select_permission, field_state) = if let Some(ref res) = resolved {
				let perm = res.select_permission(check_perms);
				let fs = res.field_state_for_projection(needed_fields.as_ref());
				(perm, fs)
			} else {
				// Check table existence and resolve SELECT permission
				let table_def = db_ctx
					.get_table_def(&table_name)
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

				let field_state = build_field_state(
					&ctx, &table_name, check_perms, needed_fields.as_ref(),
				).await?;
				(select_permission, field_state)
			};

			// Early exit if denied
			if matches!(select_permission, PhysicalPermission::Deny) {
				return;
			}

			// Build the pipeline (no predicate/limit/start — outer operators handle those)
			let mut pipeline = ScanPipeline::new(
				select_permission, None, field_state,
				check_perms, None, 0,
			);

			// Deduplicate and stream results through the permission pipeline
			let mut seen: HashSet<RecordId> = HashSet::new();
			for mut sub_stream in sub_streams {
				while let Some(batch_result) = sub_stream.next().await {
					// Check for cancellation between batches
					if ctx.cancellation().is_cancelled() {
						Err(ControlFlow::Err(
							anyhow::anyhow!(crate::err::Error::QueryCancelled),
						))?;
					}

					let batch: ValueBatch = batch_result?;
					let mut deduped: Vec<Value> = batch.values.into_iter()
						.filter(|v| {
							if let Value::Object(obj) = v
								&& let Some(Value::RecordId(rid)) = obj.get("id")
							{
								return seen.insert(rid.clone());
							}
							true // non-object values pass through
						})
						.collect();

					if !deduped.is_empty() {
						// Apply permission pipeline (computed fields, field permissions)
						let cont = pipeline.process_batch(&mut deduped, &ctx).await?;
						if !deduped.is_empty() {
							yield ValueBatch { values: deduped };
						}
						if !cont {
							return;
						}
					}
				}
			}
		});

		Ok(monitor_stream(stream, "UnionIndexScan", &self.metrics))
	}
}
