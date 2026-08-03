//! Aggregate operator for GROUP BY processing.
//!
//! Collects all input rows into groups keyed by GROUP BY expressions,
//! then applies aggregate functions (COUNT, SUM, array::group, etc.)
//! to each group. This is a pipeline-breaking operator: the entire
//! input stream must be consumed before any output is produced.

use std::collections::BTreeMap;
use std::sync::Arc;

use common::future::stream::{self, Yielder};
use futures::StreamExt;

use crate::err::EngineError;
use crate::exec::function::{Accumulator, AggregateFunction};
use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecOperator, ExecutionContext, FlowResult,
	FlowResultExt as _, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, buffer_stream,
	monitor_stream,
};
use crate::expr::idiom::Idiom;
use crate::val::{Object, Value};

/// Aggregates values by grouping keys.
///
/// GROUP BY collects all values into groups based on the specified keys,
/// then applies aggregate functions (COUNT, SUM, array::group, etc.) to each group.
///
/// This is a pipeline breaking operator - it must consume the entire input stream
/// before producing any output.
#[derive(Debug, Clone)]
pub struct Aggregate {
	pub(crate) input: Arc<dyn ExecOperator>,
	/// The original GROUP BY idioms (for display/debugging purposes).
	pub(crate) group_by: Vec<Idiom>,
	/// Physical expressions to evaluate for computing group keys.
	/// These are the actual expressions that determine grouping.
	/// For `GROUP BY country, year` where `year` is an alias for `time::year(time)`,
	/// this would contain expressions for `country` and `time::year(time)`.
	pub(crate) group_by_exprs: Vec<Arc<dyn PhysicalExpr>>,
	/// The aggregate expressions to compute for each group.
	/// These are the selected fields that may contain aggregate functions.
	pub(crate) aggregates: Vec<AggregateField>,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl Aggregate {
	/// Create a new Aggregate operator.
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		group_by: Vec<Idiom>,
		group_by_exprs: Vec<Arc<dyn PhysicalExpr>>,
		aggregates: Vec<AggregateField>,
	) -> Self {
		Self {
			input,
			group_by,
			group_by_exprs,
			aggregates,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

/// Represents a field in the SELECT that may be an aggregate.
#[derive(Debug, Clone)]
pub struct AggregateField {
	/// The output path for this field (e.g., ["address", "city"] for "address.city")
	/// This allows proper nested object construction.
	pub output_path: Vec<String>,
	/// Whether this field is a group-by key (passed through unchanged)
	pub is_group_key: bool,
	/// If this is a group-by key, the index into the group key vector.
	/// This allows retrieving the computed group key value directly.
	pub group_key_index: Option<usize>,
	/// Information about aggregate functions in this expression (if any).
	/// When set, the accumulator-based evaluation is used.
	/// Supports multiple aggregates per expression (e.g., `SUM(a) + AVG(a)`).
	pub aggregate_expr_info: Option<AggregateExprInfo>,
	/// Expression to evaluate for non-aggregate fields (e.g., group-by keys or first-value
	/// fields). This is used when aggregate_expr_info is None.
	pub fallback_expr: Option<Arc<dyn PhysicalExpr>>,
}

impl AggregateField {
	/// Create a new AggregateField from an output name string.
	///
	/// The name is treated as an opaque flat key. Callers that need a
	/// nested output path (e.g. for `AS foo.bar`) must supply the path
	/// components directly via [`AggregateField::with_output_path`] —
	/// typically by walking the parsed alias idiom's [`crate::expr::part::Part`]s
	/// so that `AS foo.bar` nests as `[foo, bar]` while
	/// `` AS `foo.bar` `` stays a single flat key `"foo.bar"`.
	pub fn new(
		name: String,
		is_group_key: bool,
		group_key_index: Option<usize>,
		aggregate_expr_info: Option<AggregateExprInfo>,
		fallback_expr: Option<Arc<dyn PhysicalExpr>>,
	) -> Self {
		Self::with_output_path(
			vec![name],
			is_group_key,
			group_key_index,
			aggregate_expr_info,
			fallback_expr,
		)
	}

	/// Create a new AggregateField from a pre-built nested output path.
	///
	/// Used when the output path was derived structurally from the parsed
	/// alias idiom, preserving the distinction between multi-part aliases
	/// (`AS foo.bar` → `["foo", "bar"]`) and single-part aliases whose
	/// identifier happens to contain a dot (`` AS `foo.bar` `` →
	/// `["foo.bar"]`).
	pub fn with_output_path(
		output_path: Vec<String>,
		is_group_key: bool,
		group_key_index: Option<usize>,
		aggregate_expr_info: Option<AggregateExprInfo>,
		fallback_expr: Option<Arc<dyn PhysicalExpr>>,
	) -> Self {
		Self {
			output_path,
			is_group_key,
			group_key_index,
			aggregate_expr_info,
			fallback_expr,
		}
	}

	/// Check if this is an empty name (used for SELECT VALUE with GROUP BY)
	pub fn is_empty_name(&self) -> bool {
		self.output_path.len() == 1 && self.output_path[0].is_empty()
	}
}

/// Information about all aggregates extracted from a single SELECT expression.
///
/// Supports expressions with multiple aggregates like `SUM(a) + AVG(a)`.
/// Each aggregate is extracted and assigned a synthetic field name (`_a0`, `_a1`, etc.).
/// The original expression is transformed to reference these fields.
#[derive(Clone)]
pub struct AggregateExprInfo {
	/// All extracted aggregate functions, indexed by their position.
	/// For `SUM(a) + AVG(a)`, this would contain `[SUM(a), AVG(a)]`.
	pub aggregates: Vec<ExtractedAggregate>,

	/// The transformed expression with aggregates replaced by field references.
	/// Uses synthetic field names like `_a0`, `_a1` that correspond to
	/// indices in the `aggregates` vector.
	/// None if the expression is a direct single aggregate (no transformation needed).
	pub post_expr: Option<Arc<dyn PhysicalExpr>>,
}

impl std::fmt::Debug for AggregateExprInfo {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("AggregateExprInfo")
			.field("num_aggregates", &self.aggregates.len())
			.field("has_post_expr", &self.post_expr.is_some())
			.finish()
	}
}

/// A single aggregate function extracted from an expression.
#[derive(Clone)]
pub struct ExtractedAggregate {
	/// The aggregate function from the registry.
	pub function: Arc<dyn AggregateFunction>,
	/// The expression to evaluate per-row to get the value to accumulate.
	/// For `math::mean(a)`, this would be the expression for `a`.
	pub argument_expr: Arc<dyn PhysicalExpr>,
	/// Additional arguments (evaluated once per group, not per-row).
	/// For `array::join(txt, " ")`, this would contain the separator expression.
	pub extra_args: Vec<Arc<dyn PhysicalExpr>>,
}

impl std::fmt::Debug for ExtractedAggregate {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("ExtractedAggregate").field("function", &self.function.name()).finish()
	}
}

/// Generate a synthetic field name for an aggregate at the given index.
/// These names are used in the transformed expression to reference aggregate results.
pub fn aggregate_field_name(idx: usize) -> String {
	format!("_a{}", idx)
}

/// Key for grouping - a tuple of values corresponding to GROUP BY expressions
type GroupKey = Vec<Value>;

/// Per-group aggregate state using accumulators
struct GroupState {
	/// Accumulators for each aggregate field.
	/// For fields with multiple aggregates (e.g., `SUM(a) + AVG(a)`),
	/// this contains a Vec of accumulators, one per extracted aggregate.
	/// Empty Vec for non-aggregate fields.
	accumulators: Vec<Vec<Box<dyn Accumulator>>>,
	/// First values seen for non-aggregate fields
	first_values: Vec<Value>,
}

// ---------------------------------------------------------------------------
// Group map
// ---------------------------------------------------------------------------
//
// Two rows belong to the same group iff their group keys compare equal under
// `Value: Ord`, so the map has to be *ordered*. Hash-partitioning the keys is
// not an option, because `Value: Hash` is not consistent with `Value: Eq` for
// numbers: `Number`'s comparison is cross-variant and, between `Float` and
// `Decimal`, approximate — `0.1f == 0.1dec` while `0.11111f != 0.11111dec` —
// whereas `Number: Hash` hashes each variant's exact decimal expansion. So
// `0.1f` and `0.1dec` bucket apart and silently form two groups. No canonical
// form reproduces that comparison, which is why the identity relation here is
// `Ord` and not a hash.
//
// Ordered keys also make the group order deterministic, and they let the drain
// feed results downstream already sorted by key.
//
// `Vec<Value>: Borrow<[Value]>`, so a row probes with the borrowed slice its key
// values already live in and nothing is cloned unless the row opens a new group.
//
// The keys are held apart from the states, mapping each key to an index into
// `states`, because a `BTreeMap<GroupKey, GroupState>` cannot answer
// get-or-insert in one descent on stable Rust: returning the `get_mut` borrow
// from the hit branch keeps it live across the insert branch (E0499, NLL problem
// case 3), so a single map costs either two descents per row (`contains_key`
// then `get_mut`) or a key clone per row (`entry(key.to_vec())`) — the clone
// being the cost this layout exists to avoid. Copying the `usize` out ends the
// borrow, so the hit path is one descent plus one indexed load.
//
// Spill-to-disk for high-cardinality groups is a future change.
struct GroupMap {
	index: BTreeMap<GroupKey, usize>,
	states: Vec<GroupState>,
}

impl GroupMap {
	fn new() -> Self {
		Self {
			index: BTreeMap::new(),
			states: Vec::new(),
		}
	}

	fn is_empty(&self) -> bool {
		self.states.is_empty()
	}

	/// Look up the state for `key`, creating it if absent.
	///
	/// `key` borrows the row's already-evaluated group-by values — empty for
	/// GROUP ALL. An owned [`GroupKey`] is built only when the row opens a new
	/// group.
	fn entry_for_row<F>(&mut self, key: &[Value], create: F) -> &mut GroupState
	where
		F: FnOnce() -> GroupState,
	{
		let idx = match self.index.get(key) {
			Some(idx) => *idx,
			None => {
				let idx = self.states.len();
				self.states.push(create());
				self.index.insert(key.to_vec(), idx);
				idx
			}
		};
		&mut self.states[idx]
	}

	/// Drain the map in group-key order.
	///
	/// `index` is the key-ordered side, so it drives the iteration and claims each
	/// state by index; every index appears exactly once, which is what lets the
	/// states be taken rather than cloned.
	fn into_key_order(self) -> impl ExactSizeIterator<Item = (GroupKey, GroupState)> {
		let mut states: Vec<Option<GroupState>> = self.states.into_iter().map(Some).collect();
		self.index
			.into_iter()
			.map(move |(key, idx)| (key, states[idx].take().expect("each index appears once")))
	}
}

/// Lay per-expression group-key columns out row-major, so each row's key is a
/// contiguous slice the group probe can borrow.
///
/// `columns[expr_idx][row_idx]` is the evaluated value for group-by expression
/// `expr_idx` at row `row_idx`. The result holds `row_count` consecutive runs of
/// `columns.len()` values, so row `i`'s key is
/// `[i * columns.len() .. (i + 1) * columns.len()]` — the one caller-visible
/// invariant, and the reason this function owns the layout rather than the call
/// site. `columns` is consumed: values are moved out, and a single column is
/// taken whole because it is already the row-major form.
fn group_key_rows(columns: &mut Vec<Vec<Value>>, row_count: usize) -> Vec<Value> {
	if columns.len() == 1 {
		return columns.swap_remove(0);
	}
	let mut flat = Vec::with_capacity(row_count * columns.len());
	for row_idx in 0..row_count {
		for col in columns.iter_mut() {
			flat.push(std::mem::take(&mut col[row_idx]));
		}
	}
	flat
}

impl ExecOperator for Aggregate {
	fn name(&self) -> &'static str {
		"Aggregate"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		use surrealdb_types::ToSql;
		if self.group_by.is_empty() {
			vec![("mode".to_string(), "GROUP ALL".to_string())]
		} else {
			vec![(
				"by".to_string(),
				self.group_by.iter().map(|i| i.to_sql()).collect::<Vec<_>>().join(", "),
			)]
		}
	}

	fn required_context(&self) -> ContextLevel {
		// Combine group-by and aggregate expression contexts with child operator context
		let group_ctx = self
			.group_by_exprs
			.iter()
			.map(|e| e.required_context())
			.max()
			.unwrap_or(ContextLevel::Root);
		let agg_ctx = self
			.aggregates
			.iter()
			.map(|agg| {
				let info_ctx = agg
					.aggregate_expr_info
					.as_ref()
					.map(|info| {
						let agg_arg_ctx = info
							.aggregates
							.iter()
							.flat_map(|ext| {
								std::iter::once(ext.argument_expr.required_context())
									.chain(ext.extra_args.iter().map(|e| e.required_context()))
							})
							.max()
							.unwrap_or(ContextLevel::Root);
						let post_ctx = info
							.post_expr
							.as_ref()
							.map(|e| e.required_context())
							.unwrap_or(ContextLevel::Root);
						agg_arg_ctx.max(post_ctx)
					})
					.unwrap_or(ContextLevel::Root);
				let fallback_ctx = agg
					.fallback_expr
					.as_ref()
					.map(|e| e.required_context())
					.unwrap_or(ContextLevel::Root);
				info_ctx.max(fallback_ctx)
			})
			.max()
			.unwrap_or(ContextLevel::Root);
		group_ctx.max(agg_ctx).max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		// Combine input's access mode with aggregate expression modes
		let mut mode = self.input.access_mode();
		// Include group-by expressions
		for expr in &self.group_by_exprs {
			mode = mode.combine(expr.access_mode());
		}
		for agg in &self.aggregates {
			if let Some(info) = &agg.aggregate_expr_info {
				// Check all extracted aggregates
				for extracted in &info.aggregates {
					mode = mode.combine(extracted.argument_expr.access_mode());
					for extra_arg in &extracted.extra_args {
						mode = mode.combine(extra_arg.access_mode());
					}
				}
				// Check post-expression
				if let Some(post_expr) = &info.post_expr {
					mode = mode.combine(post_expr.access_mode());
				}
			}
			if let Some(expr) = &agg.fallback_expr {
				mode = mode.combine(expr.access_mode());
			}
		}
		mode
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		let mut exprs = Vec::new();
		for expr in &self.group_by_exprs {
			exprs.push(("group_by", expr));
		}
		for agg in &self.aggregates {
			if let Some(info) = &agg.aggregate_expr_info {
				for extracted in &info.aggregates {
					exprs.push(("agg_arg", &extracted.argument_expr));
					for extra in &extracted.extra_args {
						exprs.push(("agg_extra", extra));
					}
				}
				if let Some(post) = &info.post_expr {
					exprs.push(("agg_post_expr", post));
				}
			}
			if let Some(fallback) = &agg.fallback_expr {
				exprs.push(("agg_fallback", fallback));
			}
		}
		exprs
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let mut input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		let group_by_exprs = self.group_by_exprs.clone();
		let aggregates = self.aggregates.clone();
		let ctx = ctx.clone();

		// Collect all input batches, then group and aggregate
		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			// Pre-evaluate extra_args for each aggregate (evaluated once, not per-row)
			// This is needed for functions like array::join(txt, " ") where " " is evaluated once
			// Structure: evaluated_extra_args[field_idx][aggregate_idx] = Vec<Value>
			let eval_ctx_for_args = EvalContext::from_exec_ctx(&ctx);
			let mut evaluated_extra_args: Vec<Vec<Vec<Value>>> =
				Vec::with_capacity(aggregates.len());
			for agg in &aggregates {
				if let Some(info) = &agg.aggregate_expr_info {
					let mut field_args = Vec::with_capacity(info.aggregates.len());
					for extracted in &info.aggregates {
						let mut args = Vec::with_capacity(extracted.extra_args.len());
						for extra_arg in &extracted.extra_args {
							let value =
								extra_arg.evaluate(eval_ctx_for_args.clone()).await.or_none()?;
							args.push(value);
						}
						field_args.push(args);
					}
					evaluated_extra_args.push(field_args);
				} else {
					evaluated_extra_args.push(vec![]);
				}
			}

			// Accumulate all values into groups. See `GroupMap` for why the group
			// identity relation has to be `Ord` rather than a hash, and how the
			// per-row probe avoids cloning the group-by values.
			let mut groups = GroupMap::new();

			// Consume all input batches
			while let Some(batch_result) = input_stream.next().await {
				// Check for cancellation between batches
				if ctx.cancellation().is_cancelled() {
					Err(crate::expr::ControlFlow::Err(anyhow::anyhow!(
						EngineError::QueryCancelled
					)))?;
				}
				let batch = batch_result?;
				let eval_ctx = EvalContext::from_exec_ctx(&ctx);

				// Phase 1: Batch evaluate group-by key expressions across all rows
				let mut group_key_columns: Vec<Vec<Value>> =
					Vec::with_capacity(group_by_exprs.len());
				for expr in &group_by_exprs {
					let keys = match expr.evaluate_batch(eval_ctx.clone(), &batch.values).await {
						Ok(v) => v,
						Err(_) => {
							// Fallback: evaluate per-row, replacing ignorable errors with None
							let mut keys = Vec::with_capacity(batch.values.len());
							for value in &batch.values {
								let v =
									expr.evaluate(eval_ctx.with_value(value)).await.or_none()?;
								keys.push(v);
							}
							keys
						}
					};
					group_key_columns.push(keys);
				}

				// Phase 2: Batch evaluate aggregate argument expressions
				let mut agg_arg_columns: Vec<Vec<Vec<Value>>> =
					Vec::with_capacity(aggregates.len());
				for agg in &aggregates {
					if let Some(info) = &agg.aggregate_expr_info {
						let mut field_cols = Vec::with_capacity(info.aggregates.len());
						for extracted in &info.aggregates {
							let col = match extracted
								.argument_expr
								.evaluate_batch(eval_ctx.clone(), &batch.values)
								.await
							{
								Ok(v) => v,
								Err(_) => {
									// Fallback: evaluate per-row, replacing ignorable errors with
									// None
									let mut col = Vec::with_capacity(batch.values.len());
									for value in &batch.values {
										let v = extracted
											.argument_expr
											.evaluate(eval_ctx.with_value(value))
											.await
											.or_none()?;
										col.push(v);
									}
									col
								}
							};
							field_cols.push(col);
						}
						agg_arg_columns.push(field_cols);
					} else {
						agg_arg_columns.push(vec![]);
					}
				}

				// Phase 3: Dispatch rows to groups and update accumulators
				if group_by_exprs.is_empty() {
					// GROUP ALL fast path: single group, pass entire columns
					// to update_batch to avoid per-row virtual dispatch.
					let state = groups.entry_for_row(&[], || {
						create_group_state(&aggregates, &evaluated_extra_args)
					});

					for (field_idx, agg) in aggregates.iter().enumerate() {
						if agg.is_group_key {
							continue;
						}

						if agg.aggregate_expr_info.is_some() {
							for (agg_idx, arg_col) in agg_arg_columns[field_idx].iter().enumerate()
							{
								if let Some(acc) = state.accumulators[field_idx].get_mut(agg_idx)
									&& let Err(e) = acc.update_batch(arg_col)
								{
									tracing::debug!(error = %e, "Accumulator batch update failed, skipping batch");
								}
							}
						} else if let Some(expr) = &agg.fallback_expr {
							// Non-aggregate field - store first value
							if state.first_values[field_idx].is_none()
								&& let Some(first_value) = batch.values.first()
							{
								match expr.evaluate(eval_ctx.with_value(first_value)).await {
									Ok(field_value) => {
										state.first_values[field_idx] = field_value;
									}
									Err(cf) if cf.is_ignorable() => {
										tracing::debug!(error = %cf, "Fallback expression evaluation failed (ignorable)");
									}
									Err(cf) => Err(cf)?,
								}
							}
						}
					}
				} else {
					// GROUP BY: per-row dispatch to separate groups. Each row's
					// key is a contiguous run in `key_rows`, so the probe borrows it
					// and only clones when the row opens a new group.
					let key_width = group_key_columns.len();
					let key_rows = group_key_rows(&mut group_key_columns, batch.values.len());

					for (row_idx, value) in batch.values.iter().enumerate() {
						let key = &key_rows[row_idx * key_width..(row_idx + 1) * key_width];
						let state = groups.entry_for_row(key, || {
							create_group_state(&aggregates, &evaluated_extra_args)
						});

						for (field_idx, agg) in aggregates.iter().enumerate() {
							if agg.is_group_key {
								continue;
							}

							if agg.aggregate_expr_info.is_some() {
								// Use pre-computed aggregate argument values
								for (agg_idx, arg_col) in
									agg_arg_columns[field_idx].iter().enumerate()
								{
									let arg_value = arg_col[row_idx].clone();
									if let Some(acc) =
										state.accumulators[field_idx].get_mut(agg_idx)
										&& let Err(e) = acc.update(arg_value)
									{
										tracing::debug!(error = %e, "Accumulator update failed, skipping value");
									}
								}
							} else if let Some(expr) = &agg.fallback_expr {
								// Non-aggregate field - store first value (per-row, lazy)
								if state.first_values[field_idx].is_none() {
									match expr.evaluate(eval_ctx.with_value(value)).await {
										Ok(field_value) => {
											state.first_values[field_idx] = field_value;
										}
										Err(cf) if cf.is_ignorable() => {
											tracing::debug!(error = %cf, "Fallback expression evaluation failed (ignorable)");
										}
										Err(cf) => Err(cf)?,
									}
								}
							}
						}
					}
				}
			}

			// GROUP ALL on empty input: produce one row with default aggregate
			// values (e.g. COUNT() = 0) when the scan ran in an authorised
			// context.  When permission checks are active and 0 rows passed
			// filtering the old compute path returns [] — replicate that.
			if group_by_exprs.is_empty() && groups.is_empty() {
				let perms_active = ctx.should_check_perms(crate::iam::Action::View).unwrap_or(true);
				if !perms_active {
					groups.entry_for_row(&[], || {
						create_group_state(&aggregates, &evaluated_extra_args)
					});
				}
			}

			// Now compute final results for each group, in group-key order.
			let ordered_groups = groups.into_key_order();
			let mut results = Vec::with_capacity(ordered_groups.len());
			for (group_key, state) in ordered_groups {
				let result =
					compute_group_result_async(&group_key, state, &aggregates, &ctx).await?;
				results.push(result);
			}

			yielder
				.emit(ValueBatch {
					values: results,
				})
				.await;
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "Aggregate", &self.metrics))
	}
}

/// Create initial group state with accumulators for each aggregate field.
///
/// The `evaluated_extra_args` parameter contains the pre-evaluated extra arguments
/// for each aggregate field: `evaluated_extra_args[field_idx][aggregate_idx] = Vec<Value>`.
fn create_group_state(
	aggregates: &[AggregateField],
	evaluated_extra_args: &[Vec<Vec<Value>>],
) -> GroupState {
	let accumulators = aggregates
		.iter()
		.enumerate()
		.map(|(i, agg)| {
			if let Some(info) = &agg.aggregate_expr_info {
				// Create an accumulator for each extracted aggregate
				info.aggregates
					.iter()
					.enumerate()
					.map(|(agg_idx, extracted)| {
						let extra_args = evaluated_extra_args
							.get(i)
							.and_then(|field_args| field_args.get(agg_idx))
							.map(|v| v.as_slice())
							.unwrap_or(&[]);
						extracted.function.create_accumulator_with_args(extra_args)
					})
					.collect()
			} else {
				// Non-aggregate field - no accumulators
				vec![]
			}
		})
		.collect();

	let first_values = aggregates.iter().map(|_| Value::None).collect();

	GroupState {
		accumulators,
		first_values,
	}
}

/// Compute the value for a single aggregate field.
///
/// This handles three cases:
/// 1. Group-by key: return the key value from the group key vector
/// 2. Aggregate expression: finalize accumulators and optionally evaluate post-expression
/// 3. Non-aggregate field: return the first value seen
async fn compute_single_field_value(
	agg: &AggregateField,
	group_key: &GroupKey,
	accumulators: &[Box<dyn Accumulator>],
	first_value: Value,
	ctx: &ExecutionContext,
) -> FlowResult<Value> {
	if let Some(idx) = agg.group_key_index {
		// For group-by keys, use the key value directly by index
		Ok(group_key.get(idx).cloned().unwrap_or(Value::None))
	} else if let Some(info) = &agg.aggregate_expr_info {
		// Compute the aggregate value(s)
		compute_aggregate_field_value(info, accumulators, ctx).await
	} else {
		// Return first value for non-aggregate fields
		Ok(first_value)
	}
}

/// Compute the result value for a single group, with support for multiple aggregates per field.
///
/// For expressions like `SUM(a) + AVG(a)`:
/// 1. Finalize all accumulators to get `{ _a0: sum_value, _a1: avg_value }`
/// 2. Evaluate the post-expression against this document to get the final value
async fn compute_group_result_async(
	group_key: &GroupKey,
	state: GroupState,
	aggregates: &[AggregateField],
	ctx: &ExecutionContext,
) -> FlowResult<Value> {
	// Special case: SELECT VALUE with GROUP BY
	// If there's exactly one aggregate with an empty name, return the raw value
	if aggregates.len() == 1 && aggregates[0].is_empty_name() {
		let agg = &aggregates[0];
		let first_value = state.first_values.into_iter().next().unwrap_or(Value::None);
		let accumulators = state.accumulators.into_iter().next().unwrap_or_default();
		return compute_single_field_value(agg, group_key, &accumulators, first_value, ctx).await;
	}

	// Normal case: return an object with field names
	let mut result = Object::default();

	// Zip aggregates with their corresponding accumulators and first values
	let field_data = aggregates.iter().zip(state.accumulators).zip(state.first_values);

	for ((agg, accumulators), first_value) in field_data {
		let field_value =
			compute_single_field_value(agg, group_key, &accumulators, first_value, ctx).await?;

		// Use nested setting to properly construct nested objects
		// e.g., path ["address", "city"] creates { address: { city: value } }
		set_nested_value(&mut result, &agg.output_path, field_value);
	}

	Ok(Value::Object(result))
}

/// Set a value at a nested path in an object.
///
/// For a path like ["address", "city"], this creates or updates:
/// `{ address: { city: value } }`
fn set_nested_value(obj: &mut Object, path: &[String], value: Value) {
	if path.is_empty() {
		return;
	}

	if path.len() == 1 {
		// Simple case: just insert at this level
		obj.insert(path[0].clone(), value);
		return;
	}

	// Need to traverse/create nested structure
	let key = &path[0];
	let rest = &path[1..];

	// Get or create the nested object
	let nested = obj.entry(key.clone()).or_insert_with(|| Value::Object(Object::default()));

	match nested {
		Value::Object(nested_obj) => {
			set_nested_value(nested_obj, rest, value);
		}
		_ => {
			// Replace non-object with new object containing the nested path
			let mut new_obj = Object::default();
			set_nested_value(&mut new_obj, rest, value);
			*nested = Value::Object(new_obj);
		}
	}
}

/// Compute the final value for a field with aggregate expressions.
///
/// If there's a post_expr, builds a document with all aggregate results
/// and evaluates the expression against it. Otherwise returns the single
/// aggregate value directly.
async fn compute_aggregate_field_value(
	info: &AggregateExprInfo,
	accumulators: &[Box<dyn Accumulator>],
	ctx: &ExecutionContext,
) -> FlowResult<Value> {
	if info.aggregates.is_empty() {
		return Ok(Value::Null);
	}

	// Finalize all accumulators and build the aggregate document
	let mut agg_doc = Object::default();
	for (idx, acc) in accumulators.iter().enumerate() {
		let value = match acc.finalize() {
			Ok(v) => v,
			Err(e) => {
				tracing::debug!(error = %e, idx, "Accumulator finalize failed, using Null");
				Value::Null
			}
		};
		agg_doc.insert(aggregate_field_name(idx), value);
	}

	// If there's a post-expression, evaluate it against the aggregate document
	if let Some(post_expr) = &info.post_expr {
		let doc_value = Value::Object(agg_doc);
		let eval_ctx = EvalContext::from_exec_ctx(ctx).with_value(&doc_value);
		match post_expr.evaluate(eval_ctx).await {
			Ok(v) => Ok(v),
			Err(cf) if cf.is_ignorable() => {
				tracing::debug!(error = %cf, "Post-expression evaluation failed (ignorable), using Null");
				Ok(Value::Null)
			}
			Err(cf) => Err(cf),
		}
	} else {
		// No post-expression means direct single aggregate - return first value
		Ok(agg_doc.0.into_values().next().unwrap_or(Value::Null))
	}
}

#[cfg(test)]
mod tests {
	use std::collections::hash_map::DefaultHasher;
	use std::hash::{Hash, Hasher};
	use std::str::FromStr;

	use rust_decimal::Decimal;

	use super::*;
	use crate::val::Number;

	fn dec(s: &str) -> Value {
		Value::Number(Number::Decimal(Decimal::from_str(s).unwrap()))
	}

	fn float(f: f64) -> Value {
		Value::Number(Number::Float(f))
	}

	fn empty_state() -> GroupState {
		create_group_state(&[], &[])
	}

	fn group_count(keys: &[Vec<Value>]) -> usize {
		let mut map = GroupMap::new();
		for key in keys {
			map.entry_for_row(key, empty_state);
		}
		map.into_key_order().len()
	}

	/// Group identity is `Value: Ord`, so every numeric variant carrying the
	/// same value shares one group.
	#[test]
	fn numerically_equal_keys_share_a_group() {
		assert_eq!(
			group_count(&[
				vec![Value::from(1i64)],
				vec![float(1.0)],
				vec![dec("1")],
				vec![dec("1.0")],
			]),
			1
		);
		assert_eq!(group_count(&[vec![float(0.1)], vec![dec("0.1")]]), 1);
		assert_eq!(group_count(&[vec![float(1.5)], vec![dec("1.50")]]), 1);
		// Multi-column keys collapse per column.
		assert_eq!(
			group_count(&[vec![Value::from(1i64), dec("2")], vec![dec("1.0"), float(2.0)],]),
			1
		);
	}

	/// The reason [`GroupMap`] is ordered rather than hash-partitioned:
	/// `Number: Hash` hashes each variant's exact decimal expansion, so it
	/// disagrees with the cross-variant `Number: PartialEq` that decides group
	/// membership. Bucketing by hash splits one group in two.
	#[test]
	fn hash_disagrees_with_the_equality_that_decides_grouping() {
		fn hash(v: &Value) -> u64 {
			let mut hasher = DefaultHasher::new();
			v.hash(&mut hasher);
			hasher.finish()
		}

		for (a, b) in [(float(0.1), dec("0.1")), (float(1.5), dec("1.50"))] {
			assert_eq!(a, b, "{a:?} and {b:?} are one group");
			assert_ne!(hash(&a), hash(&b), "{a:?} and {b:?} would bucket apart");
		}
	}

	/// Distinct keys stay distinct, and draining yields them in key order.
	#[test]
	fn distinct_keys_drain_in_key_order() {
		let mut map = GroupMap::new();
		for key in [dec("2"), float(1.5), Value::from(1i64), float(2.0), dec("1.50")] {
			map.entry_for_row(&[key], empty_state);
		}
		let keys: Vec<Value> =
			map.into_key_order().map(|(k, _)| k.into_iter().next().unwrap()).collect();
		assert_eq!(keys, vec![Value::from(1i64), float(1.5), dec("2")]);
	}

	#[test]
	fn group_key_rows_takes_a_single_column_whole() {
		let mut columns = vec![vec![Value::from(1i64), dec("2")]];
		let flat = group_key_rows(&mut columns, 2);
		assert_eq!(flat, vec![Value::from(1i64), dec("2")]);
	}

	#[test]
	fn group_key_rows_lays_rows_out_contiguously() {
		let mut columns =
			vec![vec![Value::from(1i64), Value::from(2i64)], vec![Value::from(10i64), dec("20")]];
		let flat = group_key_rows(&mut columns, 2);
		assert_eq!(flat, vec![Value::from(1i64), Value::from(10i64), Value::from(2i64), dec("20")]);
	}
}
