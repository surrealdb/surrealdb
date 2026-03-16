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
use crate::exec::field_path::FieldPath;
use crate::exec::operators::SortDirection;
use crate::exec::ordering::{OutputOrdering, SortProperty};
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
	/// When set, use a k-way merge by record ID instead of sequential
	/// iteration.  Each sub-stream must already produce results in record-ID
	/// order (which single-column equality index scans naturally do).
	/// The merge produces globally record-ID-sorted output, enabling sort
	/// elimination and early termination for ORDER BY id queries.
	pub(crate) merge_by_id: Option<SortDirection>,
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
			merge_by_id: None,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	/// Set the plan-time resolved table context.
	pub(crate) fn with_resolved(mut self, resolved: ResolvedTableContext) -> Self {
		self.resolved = Some(resolved);
		self
	}

	/// Enable k-way merge by record ID in the given direction.
	///
	/// When set, the union iterates all sub-streams simultaneously using
	/// a merge-sort on record IDs instead of draining them sequentially.
	/// This produces globally record-ID-sorted output, allowing the
	/// planner to eliminate the Sort operator for ORDER BY id queries.
	pub(crate) fn with_merge_by_id(mut self, direction: SortDirection) -> Self {
		self.merge_by_id = Some(direction);
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
		let mut attrs = vec![
			("table".to_string(), self.table_name.to_string()),
			("branches".to_string(), self.inputs.len().to_string()),
		];
		if let Some(dir) = &self.merge_by_id {
			attrs.push(("merge_by_id".to_string(), format!("{dir:?}")));
		}
		attrs
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

	fn output_ordering(&self) -> OutputOrdering {
		if let Some(direction) = self.merge_by_id {
			OutputOrdering::Sorted(vec![SortProperty {
				path: FieldPath::field("id"),
				direction,
				collate: false,
				numeric: false,
			}])
		} else {
			OutputOrdering::Unordered
		}
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
			let stream = input.execute(ctx)?;
			// In merge mode, consume sub-streams on-demand — no buffering.
			// This prevents background tasks from eagerly fetching entire
			// equality ranges when only a small number of records are
			// needed (e.g. ORDER BY id LIMIT 25).
			let sub_stream = if self.merge_by_id.is_some() {
				stream
			} else {
				buffer_stream(stream, input.access_mode(), input.cardinality_hint())
			};
			sub_streams.push(sub_stream);
		}

		// Clone for the async block
		let table_name = self.table_name.clone();
		let needed_fields = self.needed_fields.clone();
		let resolved = self.resolved.clone();
		let merge_by_id = self.merge_by_id;
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

			if let Some(merge_dir) = merge_by_id {
				// ─── K-way merge by record ID ──────────────────────────
				//
				// Each sub-stream produces records sorted by record ID
				// (guaranteed by single-column equality index scans).
				// We merge them into a single globally-sorted stream,
				// picking the min (ASC) or max (DESC) record ID at each
				// step.  Because the downstream Limit operator stops
				// consuming after N records, the merge terminates early
				// for LIMIT queries — typically reading only ~N records
				// total across all sub-streams.

				// Per-stream buffers and positions
				let k = sub_streams.len();
				let mut buffers: Vec<Vec<Value>> = Vec::with_capacity(k);
				let mut positions: Vec<usize> = Vec::with_capacity(k);

				// Initialize: get first batch from each sub-stream
				for stream in &mut sub_streams {
					if let Some(batch_result) = stream.next().await {
						let batch: ValueBatch = batch_result?;
						buffers.push(batch.values);
						positions.push(0);
					} else {
						buffers.push(Vec::new());
						positions.push(0);
					}
				}

				// Track the last yielded record ID for deduplication
				let mut last_rid: Option<RecordId> = None;

				loop {
					// Check for cancellation
					if ctx.cancellation().is_cancelled() {
						Err(ControlFlow::Err(
							anyhow::anyhow!(crate::err::Error::QueryCancelled),
						))?;
					}

					// Find the cursor with the best (min for ASC, max for DESC) record ID
					let mut best_idx: Option<usize> = None;
					let mut best_rid: Option<RecordId> = None;

					for i in 0..k {
						if positions[i] >= buffers[i].len() {
							continue; // stream exhausted or buffer drained
						}
						let rid = match &buffers[i][positions[i]] {
							Value::Object(obj) => match obj.get("id") {
								Some(Value::RecordId(r)) => r.clone(),
								_ => continue,
							},
							_ => continue,
						};
						let is_better = match &best_rid {
							None => true,
							Some(prev) => match merge_dir {
								SortDirection::Asc => rid < *prev,
								SortDirection::Desc => rid > *prev,
							},
						};
						if is_better {
							best_idx = Some(i);
							best_rid = Some(rid);
						}
					}

					let Some(idx) = best_idx else {
						break; // All streams exhausted
					};

					// Take the value and advance the cursor
					let value = buffers[idx][positions[idx]].clone();
					positions[idx] += 1;

					// Refill the buffer if it's drained
					if positions[idx] >= buffers[idx].len() {
						buffers[idx].clear();
						positions[idx] = 0;
						if let Some(batch_result) = sub_streams[idx].next().await {
							let batch: ValueBatch = batch_result?;
							buffers[idx] = batch.values;
						}
					}

					// Deduplicate: skip if same record ID as last yielded
					if let Some(ref rid) = best_rid {
						if last_rid.as_ref() == Some(rid) {
							continue;
						}
						last_rid = Some(rid.clone());
					}

					// Apply permission pipeline
					let mut batch = vec![value];
					let cont = pipeline.process_batch(&mut batch, &ctx).await?;
					if !batch.is_empty() {
						yield ValueBatch { values: batch };
					}
					if !cont {
						return;
					}
				}
			} else {
				// ─── Sequential iteration (original path) ──────────────
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
			}
		});

		Ok(monitor_stream(stream, "UnionIndexScan", &self.metrics))
	}
}
