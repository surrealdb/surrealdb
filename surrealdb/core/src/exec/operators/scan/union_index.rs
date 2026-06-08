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

use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

use futures::StreamExt;
use tracing::instrument;

use super::pipeline::{ScanPipeline, build_field_state};
use super::resolved::ResolvedTableContext;
use crate::exec::field_path::FieldPath;
use crate::exec::operators::SortDirection;
use crate::exec::ordering::{OutputOrdering, SortProperty};
use crate::exec::permission::{
	PhysicalPermission, convert_permission_to_physical_runtime, should_check_perms,
	validate_record_user_access,
};
use crate::exec::{
	AccessMode, CombineAccessModes, ContextLevel, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::iam::Action;
use crate::val::{RecordId, TableName, Value};

/// How [`UnionIndexScan`] combines per-branch sub-streams.
#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum MergeMode {
	/// K-way merge by record ID. Used when each sub-stream is already
	/// sorted by record ID (e.g. single-column equality index scans) and
	/// the downstream wants `ORDER BY id`.  Duplicate record IDs across
	/// branches are deduplicated.
	ById(SortDirection),
	/// K-way merge by an indexed field value (e.g. composite index's
	/// second column when each branch pins the first column to a
	/// distinct equality value).  Each sub-stream must already be sorted
	/// by `path` in `direction`; branches must be prefix-disjoint so no
	/// record appears twice.  Enables ORDER BY-by-suffix-column with
	/// early-stop on a LIMIT downstream.
	ByIndexKey {
		path: FieldPath,
		direction: SortDirection,
	},
	/// Like `ByIndexKey` but with record-ID deduplication via a
	/// `HashSet<RecordId>`.  Used when branches can overlap on the same
	/// record — for example, `field CONTAINSANY [a, b]` on an
	/// array-element index, where a row whose `field` array contains
	/// both `a` and `b` sits in both branches' prefix ranges.  The
	/// non-dedup `ByIndexKey` variant assumes prefix-disjoint branches
	/// and would emit such a row twice.
	ByIndexKeyDedup {
		path: FieldPath,
		direction: SortDirection,
	},
}

impl MergeMode {
	fn direction(&self) -> SortDirection {
		match self {
			MergeMode::ById(d)
			| MergeMode::ByIndexKey {
				direction: d,
				..
			}
			| MergeMode::ByIndexKeyDedup {
				direction: d,
				..
			} => *d,
		}
	}
}

/// Comparison key extracted from a row for k-way merge.
#[derive(Debug, Clone, PartialEq, Eq)]
enum MergeKey {
	Rid(RecordId),
	Field(Value),
}

impl PartialOrd for MergeKey {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for MergeKey {
	fn cmp(&self, other: &Self) -> Ordering {
		match (self, other) {
			(MergeKey::Rid(a), MergeKey::Rid(b)) => a.cmp(b),
			(MergeKey::Field(a), MergeKey::Field(b)) => a.cmp(b),
			// Mixing modes is a programmer error — treat as Equal so the
			// merge falls back to "first branch wins" rather than
			// panicking.
			_ => Ordering::Equal,
		}
	}
}

/// Extract the comparison key for a row based on the active merge mode.
///
/// Returns `None` when the row does not have a usable key (e.g. an
/// object missing the indexed field, or a non-object value).  The
/// merge skips such rows; they are emitted only when no sub-stream
/// has a usable key, in which case the loop exits.
fn extract_merge_key(mode: &MergeMode, value: &Value) -> Option<MergeKey> {
	match mode {
		MergeMode::ById(_) => match value {
			Value::Object(obj) => match obj.get("id") {
				Some(Value::RecordId(rid)) => Some(MergeKey::Rid(rid.clone())),
				_ => None,
			},
			_ => None,
		},
		MergeMode::ByIndexKey {
			path,
			..
		}
		| MergeMode::ByIndexKeyDedup {
			path,
			..
		} => Some(MergeKey::Field(path.extract(value))),
	}
}

/// Extract the record ID from a row, if present.  Used by the dedup
/// path when the merge mode is `ByIndexKeyDedup`.
fn extract_rid(value: &Value) -> Option<RecordId> {
	match value {
		Value::Object(obj) => match obj.get("id") {
			Some(Value::RecordId(rid)) => Some(rid.clone()),
			_ => None,
		},
		_ => None,
	}
}

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
	/// When set, the sub-streams are k-way merged on a comparison key
	/// instead of drained sequentially.  See [`MergeMode`].
	///
	/// The merge produces a globally-sorted output and is consumed
	/// on-demand — combined with a downstream `LIMIT` this terminates
	/// early, typically reading only ~LIMIT rows total across all
	/// sub-streams.
	pub(crate) merge: Option<MergeMode>,
	/// Hint set by the planner when the downstream pipeline contains a
	/// bounded top-k sort (e.g. `ORDER BY ... LIMIT N` with N small).
	///
	/// When true, per-sub-stream prefetch via [`buffer_stream`] is skipped
	/// because the heap will discard most rows anyway; eagerly pulling
	/// batches into background channels just wastes memory under high
	/// concurrency without improving throughput.
	pub(crate) downstream_topk: bool,
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
			merge: None,
			downstream_topk: false,
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
		self.merge = Some(MergeMode::ById(direction));
		self
	}

	/// Enable k-way merge by an indexed field value.
	///
	/// Used when each sub-stream pins the prefix column(s) of a composite
	/// index to an equality value and is therefore already sorted by the
	/// next column.  The merge produces a globally-sorted output by
	/// `path` in `direction` — letting a downstream `ORDER BY` with
	/// `LIMIT` cancel the scans after just `LIMIT` rows.
	pub(crate) fn with_merge_by_index_key(
		mut self,
		path: FieldPath,
		direction: SortDirection,
	) -> Self {
		self.merge = Some(MergeMode::ByIndexKey {
			path,
			direction,
		});
		self
	}

	/// Like `with_merge_by_index_key`, but deduplicates rows by record ID
	/// during the merge.  Required when branches can overlap on the same
	/// record (e.g. `field CONTAINSANY [a, b]` on an array-element index
	/// — a row whose array contains both values appears in both
	/// branches' prefix ranges).
	pub(crate) fn with_merge_by_index_key_dedup(
		mut self,
		path: FieldPath,
		direction: SortDirection,
	) -> Self {
		self.merge = Some(MergeMode::ByIndexKeyDedup {
			path,
			direction,
		});
		self
	}

	/// Mark the scan as feeding a bounded top-k sort. Disables eager
	/// per-sub-stream prefetch (see [`Self::downstream_topk`]).
	pub(crate) fn with_downstream_topk(mut self) -> Self {
		self.downstream_topk = true;
		self
	}
}
impl ExecOperator for UnionIndexScan {
	fn name(&self) -> &'static str {
		"UnionIndexScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![
			("table".to_string(), self.table_name.to_string()),
			("branches".to_string(), self.inputs.len().to_string()),
		];
		match &self.merge {
			Some(MergeMode::ById(dir)) => {
				attrs.push(("merge_by_id".to_string(), format!("{dir:?}")));
			}
			Some(MergeMode::ByIndexKey {
				path,
				direction,
			}) => {
				attrs.push(("merge_by_index_key".to_string(), format!("{path} {direction:?}")));
			}
			Some(MergeMode::ByIndexKeyDedup {
				path,
				direction,
			}) => {
				attrs.push((
					"merge_by_index_key_dedup".to_string(),
					format!("{path} {direction:?}"),
				));
			}
			None => {}
		}
		if self.downstream_topk {
			attrs.push(("downstream_topk".to_string(), "true".to_string()));
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
		match &self.merge {
			Some(MergeMode::ById(direction)) => OutputOrdering::Sorted(vec![SortProperty {
				path: FieldPath::field("id"),
				direction: *direction,
				collate: false,
				numeric: false,
			}]),
			Some(MergeMode::ByIndexKey {
				path,
				direction,
			})
			| Some(MergeMode::ByIndexKeyDedup {
				path,
				direction,
			}) => OutputOrdering::Sorted(vec![SortProperty {
				path: path.clone(),
				direction: *direction,
				collate: false,
				numeric: false,
			}]),
			None => OutputOrdering::Unordered,
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
			// Skip per-sub-stream prefetch when either:
			//   - merge mode is set (already consumed on-demand), or
			//   - downstream_topk is set: the heap will discard most rows anyway, so spawning
			//     background tasks to eagerly pull batches just inflates memory under high
			//     concurrency without improving throughput.
			let sub_stream = if self.merge.is_some() || self.downstream_topk {
				stream
			} else {
				buffer_stream(
					stream,
					input.access_mode(),
					input.cardinality_hint(),
					ctx.root().ctx.config.operator_buffer_size,
				)
			};
			sub_streams.push(sub_stream);
		}

		// Clone for the async block
		let table_name = self.table_name.clone();
		let needed_fields = self.needed_fields.clone();
		let resolved = self.resolved.clone();
		let merge = self.merge.clone();
		let ctx = ctx.clone();

		let stream: ValueBatchStream = Box::pin(async_stream::try_stream! {
			let db_ctx = ctx.database().context("UnionIndexScan requires database context")?;
			let version = ctx.version_stamp();

			// Resolve table permissions and field state: plan-time fast path or runtime fallback
			let (select_permission, field_state) = if let Some(ref res) = resolved {
				let perm = res.select_permission(check_perms);
				let fs = res.field_state_for_projection(needed_fields.as_ref());
				(perm, fs)
			} else {
				// Check table existence and resolve SELECT permission
				let table_def = db_ctx
					.get_table_def(&table_name, version)
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
					convert_permission_to_physical_runtime(&catalog_perm, ctx.ctx())
						.await
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

			if let Some(merge_mode) = merge {
				// ─── K-way merge of sub-streams ────────────────────────
				//
				// Each sub-stream produces records already sorted by the
				// merge key (record ID for [`MergeMode::ById`], an indexed
				// field value for [`MergeMode::ByIndexKey`]).  We merge
				// them into a single globally-sorted stream by repeatedly
				// picking the cursor with the best key and yielding it.
				//
				// The merge consumes sub-streams on-demand, so a
				// downstream Limit operator terminates the scan after N
				// records — typically reading only ~N records total
				// across all sub-streams instead of draining every
				// branch.

				let direction = merge_mode.direction();
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

				// Track the last yielded record ID for `ById`-mode
				// deduplication (record IDs are monotonic across the
				// merged stream).  `ByIndexKey` branches are
				// prefix-disjoint by construction, so no dedupe is
				// needed.  `ByIndexKeyDedup` branches *can* overlap on
				// the same record (CONTAINSANY style); for that mode we
				// also keep a full `HashSet` of yielded rids.
				//
				// Memory note: `seen_rids` grows unbounded with the
				// result size for `ByIndexKeyDedup` (and the
				// `seen` set in the sequential path below has the
				// same property — pre-existing behaviour, not a
				// regression from `ByIndexKeyDedup`).  Worth
				// revisiting if this operator surfaces memory
				// pressure on very large unbounded containment
				// result sets.
				let mut last_rid: Option<RecordId> = None;
				let needs_rid_dedup = matches!(merge_mode, MergeMode::ByIndexKeyDedup { .. });
				let mut seen_rids: HashSet<RecordId> = HashSet::new();

				loop {
					// Check for cancellation
					if ctx.cancellation().is_cancelled() {
						Err(ControlFlow::Err(
							anyhow::anyhow!(crate::err::Error::QueryCancelled),
						))?;
					}

					// Find the cursor with the best key.
					// For `Asc`, "best" means minimum; for `Desc`, maximum.
					let mut best_idx: Option<usize> = None;
					let mut best_key: Option<MergeKey> = None;

					for i in 0..k {
						if positions[i] >= buffers[i].len() {
							continue; // stream exhausted or buffer drained
						}
						let key = match extract_merge_key(
							&merge_mode,
							&buffers[i][positions[i]],
						) {
							Some(k) => k,
							None => continue,
						};
						let is_better = match &best_key {
							None => true,
							Some(prev) => match direction {
								SortDirection::Asc => key.cmp(prev) == Ordering::Less,
								SortDirection::Desc => key.cmp(prev) == Ordering::Greater,
							},
						};
						if is_better {
							best_idx = Some(i);
							best_key = Some(key);
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

					// In `ById` mode the same record can appear in
					// multiple branches (OR-dedup); skip if same record
					// ID as last yielded.
					if matches!(merge_mode, MergeMode::ById(_))
						&& let Some(MergeKey::Rid(ref rid)) = best_key
					{
						if last_rid.as_ref() == Some(rid) {
							continue;
						}
						last_rid = Some(rid.clone());
					}

					// In `ByIndexKeyDedup` mode a record can also appear
					// in multiple branches but they are sorted by field
					// value, not by rid, so non-consecutive duplicates
					// are possible.  Use a full `HashSet` to drop them.
					if needs_rid_dedup
						&& let Some(rid) = extract_rid(&value)
						&& !seen_rids.insert(rid)
					{
						continue;
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
						let mut deduped: Vec<Value> = batch
							.values
							.into_iter()
							.filter(|v| match extract_rid(v) {
								Some(rid) => seen.insert(rid),
								None => true, // non-object values pass through
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
