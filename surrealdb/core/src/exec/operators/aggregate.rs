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
	use crate::exec::field_path::FieldPath;
	use crate::exec::operators::test_util::{
		ValuesOperator, collect, parse_expr, physical_expr, root_ctx, root_ctx_with_auth,
		try_collect, val,
	};
	use crate::exec::operators::{SortDirection, Union};
	use crate::exec::ordering::SortProperty;
	use crate::exec::{CardinalityHint, OutputOrdering};
	use crate::expr::{ControlFlow, Expr};
	use crate::iam::{Action, Auth, Role};
	use crate::val::Number;

	// =========================================================================
	// Builders
	// =========================================================================

	/// Build the operator over `input`. `group_by` holds the GROUP BY sources;
	/// an empty slice is GROUP ALL. Non-idiom sources are given a display idiom
	/// spelled from the source text, the way the planner does for computed keys.
	async fn aggregate_over(
		input: Arc<dyn ExecOperator>,
		group_by: &[&str],
		fields: Vec<AggregateField>,
		ctx: &ExecutionContext,
	) -> Arc<dyn ExecOperator> {
		let mut idioms = Vec::with_capacity(group_by.len());
		let mut exprs = Vec::with_capacity(group_by.len());
		for src in group_by {
			idioms.push(match parse_expr(src) {
				Expr::Idiom(idiom) => idiom,
				_ => Idiom::field(src.to_string()),
			});
			exprs.push(physical_expr(src, ctx).await);
		}
		Arc::new(Aggregate::new(input, idioms, exprs, fields))
	}

	/// Input rows from SurrealQL object literals.
	async fn rows(srcs: &[&str]) -> Vec<Value> {
		let mut out = Vec::with_capacity(srcs.len());
		for src in srcs {
			out.push(val(src).await);
		}
		out
	}

	/// A field holding one aggregate call and no post-expression — the shape the
	/// planner produces for a bare `func(arg)` selector.
	fn single(name: &str, extracted: ExtractedAggregate) -> AggregateField {
		AggregateField::new(
			name.to_string(),
			false,
			None,
			Some(AggregateExprInfo {
				aggregates: vec![extracted],
				post_expr: None,
			}),
			None,
		)
	}

	/// `count()` — the argument-less form. The planner compiles a NONE literal as
	/// the per-row argument, so the accumulator ticks once per row whatever the
	/// row holds.
	async fn count_star(name: &str, ctx: &ExecutionContext) -> AggregateField {
		single(
			name,
			ExtractedAggregate {
				function: ctx.function_registry().get_count_aggregate(false),
				argument_expr: physical_expr("NONE", ctx).await,
				extra_args: vec![],
			},
		)
	}

	/// `count(arg)` — the truthy-counting form.
	async fn count_of(name: &str, arg: &str, ctx: &ExecutionContext) -> AggregateField {
		single(
			name,
			ExtractedAggregate {
				function: ctx.function_registry().get_count_aggregate(true),
				argument_expr: physical_expr(arg, ctx).await,
				extra_args: vec![],
			},
		)
	}

	/// A registry aggregate applied to one per-row argument, e.g.
	/// `agg_of("total", "math::sum", "score", ctx)`.
	async fn agg_of(name: &str, func: &str, arg: &str, ctx: &ExecutionContext) -> AggregateField {
		single(
			name,
			ExtractedAggregate {
				function: aggregate_fn(func, ctx),
				argument_expr: physical_expr(arg, ctx).await,
				extra_args: vec![],
			},
		)
	}

	fn aggregate_fn(func: &str, ctx: &ExecutionContext) -> Arc<dyn AggregateFunction> {
		Arc::clone(
			ctx.function_registry().get_aggregate(func).expect("aggregate should be registered"),
		)
	}

	/// A pass-through field that republishes group key `idx`.
	fn key(name: &str, idx: usize) -> AggregateField {
		AggregateField::new(name.to_string(), true, Some(idx), None, None)
	}

	/// A non-aggregate field: the first non-NONE value seen in the group.
	async fn first_value(name: &str, src: &str, ctx: &ExecutionContext) -> AggregateField {
		AggregateField::new(
			name.to_string(),
			false,
			None,
			None,
			Some(physical_expr(src, ctx).await),
		)
	}

	/// Read a top-level field from an output row.
	fn field(row: &Value, name: &str) -> Value {
		match row {
			Value::Object(o) => o.get(name).cloned().unwrap_or(Value::None),
			other => panic!("expected an object row, got {other:?}"),
		}
	}

	/// A source that replays a fixed script of batches and signals. Single-use:
	/// `execute` takes the script, so each instance may be executed once.
	struct ScriptedSource {
		script: std::sync::Mutex<Option<Vec<FlowResult<ValueBatch>>>>,
		access_mode: AccessMode,
	}

	impl ScriptedSource {
		/// Returns a trait object rather than `Self`: every operator builder in
		/// exec hands back `Arc<dyn ExecOperator>`.
		#[allow(clippy::new_ret_no_self)]
		fn new(script: Vec<FlowResult<ValueBatch>>) -> Arc<dyn ExecOperator> {
			Arc::new(Self {
				script: std::sync::Mutex::new(Some(script)),
				access_mode: AccessMode::ReadOnly,
			})
		}

		fn read_write(script: Vec<FlowResult<ValueBatch>>) -> Arc<dyn ExecOperator> {
			Arc::new(Self {
				script: std::sync::Mutex::new(Some(script)),
				access_mode: AccessMode::ReadWrite,
			})
		}
	}

	impl std::fmt::Debug for ScriptedSource {
		fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
			f.write_str("ScriptedSource")
		}
	}

	impl ExecOperator for ScriptedSource {
		fn name(&self) -> &'static str {
			"ScriptedSource"
		}

		fn required_context(&self) -> ContextLevel {
			ContextLevel::Root
		}

		fn access_mode(&self) -> AccessMode {
			self.access_mode
		}

		fn execute(&self, _ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
			let script = self
				.script
				.lock()
				.expect("script lock")
				.take()
				.expect("ScriptedSource is single-use");
			Ok(Box::pin(futures::stream::iter(script)))
		}
	}

	fn batch(values: Vec<Value>) -> FlowResult<ValueBatch> {
		Ok(ValueBatch {
			values,
		})
	}

	// =========================================================================
	// Grouping
	// =========================================================================

	#[tokio::test]
	async fn rows_collapse_into_one_output_row_per_group_key() {
		let ctx = root_ctx();
		let input = ValuesOperator::new(
			rows(&[
				"{ country: 'us', score: 1 }",
				"{ country: 'de', score: 2 }",
				"{ country: 'us', score: 3 }",
				"{ country: 'us', score: 4 }",
			])
			.await,
		);
		let op = aggregate_over(
			input,
			&["country"],
			vec![
				key("country", 0),
				count_star("total", &ctx).await,
				agg_of("sum", "math::sum", "score", &ctx).await,
			],
			&ctx,
		)
		.await;

		let out = collect(&op, &ctx).await;
		assert_eq!(
			out,
			rows(&["{ country: 'de', total: 1, sum: 2 }", "{ country: 'us', total: 3, sum: 8 }"])
				.await
		);
	}

	#[tokio::test]
	async fn multi_field_group_keys_partition_on_the_whole_tuple() {
		let ctx = root_ctx();
		let input = ValuesOperator::new(
			rows(&[
				"{ c: 'us', y: 2020 }",
				"{ c: 'us', y: 2021 }",
				"{ c: 'de', y: 2020 }",
				"{ c: 'us', y: 2020 }",
			])
			.await,
		);
		let op = aggregate_over(
			input,
			&["c", "y"],
			vec![key("c", 0), key("y", 1), count_star("total", &ctx).await],
			&ctx,
		)
		.await;

		// Three distinct (c, y) tuples. The key tuple orders the output: the
		// second component only breaks ties within an equal first component.
		let out = collect(&op, &ctx).await;
		assert_eq!(
			out,
			rows(&[
				"{ c: 'de', y: 2020, total: 1 }",
				"{ c: 'us', y: 2020, total: 2 }",
				"{ c: 'us', y: 2021, total: 1 }",
			])
			.await
		);
	}

	#[tokio::test]
	async fn a_row_missing_a_group_field_groups_under_none() {
		let ctx = root_ctx();
		let input = ValuesOperator::new(
			rows(&["{ c: 'us' }", "{ score: 1 }", "{ score: 2 }", "{ c: 'us' }"]).await,
		);
		let op =
			aggregate_over(input, &["c"], vec![key("c", 0), count_star("total", &ctx).await], &ctx)
				.await;

		// The two rows without `c` share the NONE key, and NONE sorts before
		// every other value.
		let out = collect(&op, &ctx).await;
		assert_eq!(out.len(), 2);
		assert_eq!(field(&out[0], "c"), Value::None);
		assert_eq!(field(&out[0], "total"), Value::from(2));
		assert_eq!(field(&out[1], "c"), Value::from("us"));
		assert_eq!(field(&out[1], "total"), Value::from(2));
	}

	#[tokio::test]
	async fn none_and_null_group_keys_stay_distinct_groups() {
		let ctx = root_ctx();
		let input =
			ValuesOperator::new(rows(&["{ c: NULL }", "{ }", "{ c: 'us' }", "{ c: NULL }"]).await);
		let op =
			aggregate_over(input, &["c"], vec![key("c", 0), count_star("total", &ctx).await], &ctx)
				.await;

		// NONE and NULL are different values, so they never share a group; the
		// output follows Value's total order, NONE < NULL < string.
		let out = collect(&op, &ctx).await;
		assert_eq!(out.len(), 3);
		assert_eq!(field(&out[0], "c"), Value::None);
		assert_eq!(field(&out[0], "total"), Value::from(1));
		assert_eq!(field(&out[1], "c"), Value::Null);
		assert_eq!(field(&out[1], "total"), Value::from(2));
		assert_eq!(field(&out[2], "c"), Value::from("us"));
		assert_eq!(field(&out[2], "total"), Value::from(1));
	}

	// =========================================================================
	// GROUP ALL
	// =========================================================================

	#[tokio::test]
	async fn group_all_folds_every_row_into_a_single_output_row() {
		let ctx = root_ctx();
		let input =
			ValuesOperator::new(rows(&["{ score: 1 }", "{ score: 2 }", "{ score: 3 }"]).await);
		let op = aggregate_over(
			input,
			&[],
			vec![count_star("total", &ctx).await, agg_of("sum", "math::sum", "score", &ctx).await],
			&ctx,
		)
		.await;

		let out = collect(&op, &ctx).await;
		assert_eq!(out, rows(&["{ total: 3, sum: 6 }"]).await);
	}

	#[tokio::test]
	async fn group_all_with_no_input_batches_emits_the_accumulator_identities_when_perms_are_off() {
		// An input that never yields a batch leaves the group map empty. With
		// permission enforcement off, the operator then synthesises the single
		// group, so `count()` reports 0 rather than yielding no row at all.
		let ctx = root_ctx_with_auth(Auth::for_root(Role::Owner));
		assert!(!ctx.should_check_perms(Action::View).unwrap());

		let op = aggregate_over(
			ScriptedSource::new(vec![]),
			&[],
			vec![count_star("total", &ctx).await, agg_of("sum", "math::sum", "score", &ctx).await],
			&ctx,
		)
		.await;

		let out = collect(&op, &ctx).await;
		assert_eq!(out, rows(&["{ total: 0, sum: 0 }"]).await);
	}

	#[tokio::test]
	async fn group_all_with_no_input_batches_emits_nothing_when_perms_are_on() {
		// Under an identity whose reads are permission-checked, zero rows cannot
		// be distinguished from "everything was filtered out", so the identity
		// row is suppressed and the output is empty.
		let ctx = root_ctx();
		assert!(ctx.should_check_perms(Action::View).unwrap());

		let op = aggregate_over(
			ScriptedSource::new(vec![]),
			&[],
			vec![count_star("total", &ctx).await],
			&ctx,
		)
		.await;

		assert!(collect(&op, &ctx).await.is_empty());
	}

	#[tokio::test]
	async fn group_all_emits_the_identity_row_for_an_empty_batch_whatever_the_perms() {
		// The permission gate above only guards the *no batch at all* case. As
		// soon as one batch arrives the GROUP ALL branch creates its single group
		// before looking at any row, so an input that yields one empty batch
		// produces `count() = 0` even while permission checks are active.
		let ctx = root_ctx();
		assert!(ctx.should_check_perms(Action::View).unwrap());

		let op = aggregate_over(
			ScriptedSource::new(vec![batch(vec![])]),
			&[],
			vec![count_star("total", &ctx).await],
			&ctx,
		)
		.await;

		assert_eq!(collect(&op, &ctx).await, rows(&["{ total: 0 }"]).await);
	}

	#[tokio::test]
	async fn group_by_over_empty_input_emits_nothing() {
		// The identity-row fallback is GROUP ALL only: with grouping keys and no
		// rows there is no group to report.
		let ctx = root_ctx_with_auth(Auth::for_root(Role::Owner));
		let op = aggregate_over(
			ScriptedSource::new(vec![batch(vec![])]),
			&["c"],
			vec![key("c", 0), count_star("total", &ctx).await],
			&ctx,
		)
		.await;

		assert!(collect(&op, &ctx).await.is_empty());
	}

	// =========================================================================
	// Group ordering
	// =========================================================================

	#[tokio::test]
	async fn output_groups_are_ordered_by_group_key_not_by_arrival() {
		let ctx = root_ctx();
		let input = ValuesOperator::new(
			rows(&["{ c: 'us' }", "{ c: 'de' }", "{ c: 'za' }", "{ c: 'fr' }", "{ c: 'de' }"])
				.await,
		);
		let op =
			aggregate_over(input, &["c"], vec![key("c", 0), count_star("total", &ctx).await], &ctx)
				.await;

		let keys: Vec<Value> = collect(&op, &ctx).await.iter().map(|r| field(r, "c")).collect();
		assert_eq!(
			keys,
			vec![Value::from("de"), Value::from("fr"), Value::from("us"), Value::from("za")]
		);
	}

	#[tokio::test]
	async fn numerically_equal_group_keys_of_different_numeric_types_share_one_group() {
		let ctx = root_ctx();
		let input = ValuesOperator::new(rows(&["{ n: 1 }", "{ n: 1.0 }", "{ n: 1.0dec }"]).await);
		let op =
			aggregate_over(input, &["n"], vec![key("n", 0), count_star("total", &ctx).await], &ctx)
				.await;

		// Int 1, Float 1.0 and Decimal 1.0 are equal under Value's equality, and
		// Value's hash canonicalises every numeric variant through the same
		// decimal encoding, so all three rows hash into one bucket and fold into
		// one group.
		let out = collect(&op, &ctx).await;
		assert_eq!(out.len(), 1);
		assert_eq!(field(&out[0], "total"), Value::from(3));
		// The published key is the value carried by the row that created the
		// group — the first one seen, with its original numeric variant.
		assert_eq!(field(&out[0], "n"), val("1").await);
		assert!(matches!(field(&out[0], "n"), Value::Number(crate::val::Number::Int(1))));
	}

	// =========================================================================
	// Field shapes
	// =========================================================================

	#[tokio::test]
	async fn aggregate_and_group_key_fields_share_one_output_object_under_their_own_names() {
		let ctx = root_ctx();
		let input =
			ValuesOperator::new(rows(&["{ c: 'us', score: 2 }", "{ c: 'us', score: 4 }"]).await);
		// Output names are independent of the source fields: the group key is
		// republished as `country` and the sum as `total`.
		let op = aggregate_over(
			input,
			&["c"],
			vec![key("country", 0), agg_of("total", "math::sum", "score", &ctx).await],
			&ctx,
		)
		.await;

		let out = collect(&op, &ctx).await;
		assert_eq!(out, rows(&["{ country: 'us', total: 6 }"]).await);
	}

	#[tokio::test]
	async fn output_paths_nest_while_a_single_dotted_key_stays_flat() {
		let ctx = root_ctx();
		let input = ValuesOperator::new(rows(&["{ score: 2 }", "{ score: 4 }"]).await);

		// A multi-part path nests into objects; a one-element path is an opaque
		// flat key even when the identifier contains a dot.
		let sum = agg_of("ignored", "math::sum", "score", &ctx).await;
		let nested = AggregateField::with_output_path(
			vec!["stats".to_string(), "total".to_string()],
			false,
			None,
			sum.aggregate_expr_info,
			None,
		);
		let flat = count_star("stats.count", &ctx).await;

		let op = aggregate_over(input, &[], vec![nested, flat], &ctx).await;
		let out = collect(&op, &ctx).await;
		assert_eq!(out.len(), 1);
		assert_eq!(field(&out[0], "stats"), val("{ total: 6 }").await);
		assert_eq!(field(&out[0], "stats.count"), Value::from(2));
	}

	#[tokio::test]
	async fn a_single_empty_named_field_returns_the_bare_value_not_an_object() {
		let ctx = root_ctx();
		let input = ValuesOperator::new(rows(&["{ score: 2 }", "{ score: 4 }"]).await);
		// SELECT VALUE with GROUP BY: exactly one field, with an empty name.
		let only = agg_of("", "math::sum", "score", &ctx).await;
		assert!(only.is_empty_name());

		let op = aggregate_over(input, &[], vec![only], &ctx).await;
		assert_eq!(collect(&op, &ctx).await, vec![Value::from(6)]);
	}

	#[tokio::test]
	async fn an_aggregate_over_a_field_absent_from_some_rows_skips_the_missing_values() {
		let ctx = root_ctx();
		let input = ValuesOperator::new(
			rows(&["{ c: 'us', score: 2 }", "{ c: 'us' }", "{ c: 'us', score: 5 }"]).await,
		);
		let op = aggregate_over(
			input,
			&["c"],
			vec![
				count_star("rows", &ctx).await,
				count_of("scored", "score", &ctx).await,
				agg_of("sum", "math::sum", "score", &ctx).await,
			],
			&ctx,
		)
		.await;

		// `count()` counts rows, `count(score)` counts truthy values only, and
		// the sum ignores the NONE that the missing field evaluates to.
		let out = collect(&op, &ctx).await;
		assert_eq!(out, rows(&["{ rows: 3, scored: 2, sum: 7 }"]).await);
	}

	#[tokio::test]
	async fn several_aggregates_in_one_field_are_combined_by_the_post_expression() {
		let ctx = root_ctx();
		let input =
			ValuesOperator::new(rows(&["{ score: 1 }", "{ score: 2 }", "{ score: 3 }"]).await);
		// `math::sum(score) + count()`: both accumulate per row, then the
		// post-expression runs once per group against `{ _a0: sum, _a1: count }`.
		assert_eq!(aggregate_field_name(0), "_a0");
		assert_eq!(aggregate_field_name(1), "_a1");
		let combined = AggregateField::new(
			"mixed".to_string(),
			false,
			None,
			Some(AggregateExprInfo {
				aggregates: vec![
					ExtractedAggregate {
						function: aggregate_fn("math::sum", &ctx),
						argument_expr: physical_expr("score", &ctx).await,
						extra_args: vec![],
					},
					ExtractedAggregate {
						function: ctx.function_registry().get_count_aggregate(false),
						argument_expr: physical_expr("NONE", &ctx).await,
						extra_args: vec![],
					},
				],
				post_expr: Some(physical_expr("_a0 + _a1", &ctx).await),
			}),
			None,
		);

		let op = aggregate_over(input, &[], vec![combined], &ctx).await;
		assert_eq!(collect(&op, &ctx).await, rows(&["{ mixed: 9 }"]).await);
	}

	#[tokio::test]
	async fn a_field_whose_aggregate_list_is_empty_yields_null() {
		let ctx = root_ctx();
		let input = ValuesOperator::new(rows(&["{ score: 1 }"]).await);
		let empty = AggregateField::new(
			"nothing".to_string(),
			false,
			None,
			Some(AggregateExprInfo {
				aggregates: vec![],
				post_expr: None,
			}),
			None,
		);

		let op = aggregate_over(input, &[], vec![empty], &ctx).await;
		let out = collect(&op, &ctx).await;
		assert_eq!(field(&out[0], "nothing"), Value::Null);
	}

	#[tokio::test]
	async fn extra_args_are_evaluated_once_and_reused_by_every_group() {
		let ctx = root_ctx();
		let input = ValuesOperator::new(
			rows(&["{ c: 'a', w: 'x' }", "{ c: 'b', w: 'z' }", "{ c: 'a', w: 'y' }"]).await,
		);
		// `array::join(w, '-')`: the separator is an extra argument, evaluated
		// once before any row is read and handed to each group's accumulator.
		let joined = single(
			"joined",
			ExtractedAggregate {
				function: aggregate_fn("array::join", &ctx),
				argument_expr: physical_expr("w", &ctx).await,
				extra_args: vec![physical_expr("'-'", &ctx).await],
			},
		);

		let op = aggregate_over(input, &["c"], vec![key("c", 0), joined], &ctx).await;
		let out = collect(&op, &ctx).await;
		assert_eq!(out, rows(&["{ c: 'a', joined: 'x-y' }", "{ c: 'b', joined: 'z' }"]).await);
	}

	#[tokio::test]
	async fn extra_args_are_evaluated_without_a_current_row() {
		let ctx = root_ctx();
		let input =
			ValuesOperator::new(rows(&["{ w: 'x', sep: '-' }", "{ w: 'y', sep: '-' }"]).await);
		// Extra arguments are evaluated against a row-less EvalContext, so a
		// row-dependent separator resolves to NONE and the accumulator receives
		// its raw string form.
		let joined = single(
			"joined",
			ExtractedAggregate {
				function: aggregate_fn("array::join", &ctx),
				argument_expr: physical_expr("w", &ctx).await,
				extra_args: vec![physical_expr("sep", &ctx).await],
			},
		);

		let op = aggregate_over(input, &[], vec![joined], &ctx).await;
		let out = collect(&op, &ctx).await;
		assert_eq!(field(&out[0], "joined"), Value::from("xNONEy"));
	}

	#[tokio::test]
	async fn a_non_aggregate_field_reports_the_first_non_none_value_in_the_group() {
		let ctx = root_ctx();
		let input = ValuesOperator::new(
			rows(&["{ c: 'us' }", "{ c: 'us', label: 'second' }", "{ c: 'us', label: 'third' }"])
				.await,
		);
		let op = aggregate_over(
			input,
			&["c"],
			vec![key("c", 0), first_value("label", "label", &ctx).await],
			&ctx,
		)
		.await;

		// The per-row GROUP BY path keeps evaluating until it stores a non-NONE
		// value, so the first row's missing `label` does not win.
		let out = collect(&op, &ctx).await;
		assert_eq!(out, rows(&["{ c: 'us', label: 'second' }"]).await);
	}

	#[tokio::test]
	async fn group_all_first_value_fields_only_read_the_first_row_of_each_batch() {
		let ctx = root_ctx();
		// One batch whose first row lacks the field: the GROUP ALL path evaluates
		// the first-value expression against `batch.values.first()` only, so the
		// later row that does carry the field is never consulted for that batch.
		let one_batch = ValuesOperator::new(rows(&["{ }", "{ label: 'second' }"]).await);
		let op =
			aggregate_over(one_batch, &[], vec![first_value("label", "label", &ctx).await], &ctx)
				.await;
		let out = collect(&op, &ctx).await;
		assert_eq!(field(&out[0], "label"), Value::None);

		// Split across two batches, the second batch's first row does fill it.
		let two_batches = ScriptedSource::new(vec![
			batch(rows(&["{ }"]).await),
			batch(rows(&["{ label: 'second' }"]).await),
		]);
		let op =
			aggregate_over(two_batches, &[], vec![first_value("label", "label", &ctx).await], &ctx)
				.await;
		let out = collect(&op, &ctx).await;
		assert_eq!(field(&out[0], "label"), Value::from("second"));
	}

	// =========================================================================
	// Batching
	// =========================================================================

	#[tokio::test]
	async fn accumulator_state_carries_across_batch_boundaries() {
		let ctx = root_ctx();
		// Union emits each input's batch separately, so the aggregate sees two
		// batches; groups and their accumulators must persist between them.
		let input: Arc<dyn ExecOperator> = Arc::new(Union::new(vec![
			ValuesOperator::new(rows(&["{ c: 'us', score: 1 }", "{ c: 'de', score: 2 }"]).await),
			ValuesOperator::new(rows(&["{ c: 'us', score: 3 }"]).await),
		]));
		let op = aggregate_over(
			input,
			&["c"],
			vec![
				key("c", 0),
				count_star("total", &ctx).await,
				agg_of("sum", "math::sum", "score", &ctx).await,
			],
			&ctx,
		)
		.await;

		// `us` appears in both batches and still yields a single group whose
		// count and sum span them.
		let out = collect(&op, &ctx).await;
		assert_eq!(
			out,
			rows(&["{ c: 'de', total: 1, sum: 2 }", "{ c: 'us', total: 2, sum: 4 }"]).await
		);
	}

	#[tokio::test]
	async fn group_all_accumulates_over_every_batch_and_emits_one_row() {
		let ctx = root_ctx();
		let input: Arc<dyn ExecOperator> = Arc::new(Union::new(vec![
			ValuesOperator::new(rows(&["{ score: 1 }", "{ score: 2 }"]).await),
			ValuesOperator::new(rows(&["{ score: 3 }"]).await),
			ValuesOperator::new(rows(&["{ score: 4 }"]).await),
		]));
		let op = aggregate_over(
			input,
			&[],
			vec![count_star("total", &ctx).await, agg_of("sum", "math::sum", "score", &ctx).await],
			&ctx,
		)
		.await;

		// The GROUP ALL path feeds whole columns to `update_batch`; the single
		// group's accumulators are reused for every batch.
		assert_eq!(collect(&op, &ctx).await, rows(&["{ total: 4, sum: 10 }"]).await);
	}

	#[tokio::test]
	async fn empty_batches_around_a_populated_one_do_not_disturb_the_groups() {
		let ctx = root_ctx();
		let input = ScriptedSource::new(vec![
			batch(vec![]),
			batch(rows(&["{ c: 'us' }", "{ c: 'us' }"]).await),
			batch(vec![]),
		]);
		let op =
			aggregate_over(input, &["c"], vec![key("c", 0), count_star("total", &ctx).await], &ctx)
				.await;

		assert_eq!(collect(&op, &ctx).await, rows(&["{ c: 'us', total: 2 }"]).await);
	}

	// =========================================================================
	// Control flow and errors
	// =========================================================================

	#[tokio::test]
	async fn an_input_error_aborts_the_aggregate_without_emitting_a_batch() {
		let ctx = root_ctx();
		let input = ScriptedSource::new(vec![
			batch(rows(&["{ c: 'us' }"]).await),
			Err(ControlFlow::Err(anyhow::anyhow!("input exploded"))),
		]);
		let op =
			aggregate_over(input, &["c"], vec![key("c", 0), count_star("total", &ctx).await], &ctx)
				.await;

		// Pipeline-breaking: nothing has been emitted when the error arrives, so
		// the partial groups are discarded rather than reported.
		let err = try_collect(&op, &ctx).await.expect_err("input error propagates");
		assert!(err.to_string().contains("input exploded"), "unexpected error: {err}");
	}

	#[tokio::test]
	async fn control_flow_signals_from_the_input_propagate_unchanged() {
		let ctx = root_ctx();
		for signal in
			[ControlFlow::Return(Value::from(7)), ControlFlow::Break, ControlFlow::Continue]
		{
			let expected = format!("{signal:?}");
			let input = ScriptedSource::new(vec![batch(rows(&["{ c: 'us' }"]).await), Err(signal)]);
			let op = aggregate_over(
				input,
				&["c"],
				vec![key("c", 0), count_star("total", &ctx).await],
				&ctx,
			)
			.await;

			// RETURN/BREAK/CONTINUE are not errors and must not be converted into
			// one, nor swallowed into an empty result.
			let err = try_collect(&op, &ctx).await.expect_err("signal propagates");
			assert_eq!(format!("{err:?}"), expected);
		}
	}

	#[tokio::test]
	async fn a_non_ignorable_error_in_an_aggregate_argument_propagates() {
		let ctx = root_ctx();
		let input = ValuesOperator::new(rows(&["{ score: 1 }"]).await);
		let op = aggregate_over(
			input,
			&[],
			vec![agg_of("sum", "math::sum", "THROW 'boom'", &ctx).await],
			&ctx,
		)
		.await;

		let err = try_collect(&op, &ctx).await.expect_err("a thrown error is not ignorable");
		assert!(err.to_string().contains("boom"), "unexpected error: {err}");
	}

	#[tokio::test]
	async fn a_non_ignorable_error_in_a_group_key_propagates() {
		let ctx = root_ctx();
		let input = ValuesOperator::new(rows(&["{ c: 'us' }"]).await);
		let op = aggregate_over(
			input,
			&["THROW 'boom'"],
			vec![key("c", 0), count_star("total", &ctx).await],
			&ctx,
		)
		.await;

		let err = try_collect(&op, &ctx).await.expect_err("a thrown error is not ignorable");
		assert!(err.to_string().contains("boom"), "unexpected error: {err}");
	}

	#[tokio::test]
	async fn an_ignorable_error_in_a_group_key_groups_the_row_under_none() {
		let ctx = root_ctx();
		let input =
			ValuesOperator::new(rows(&["{ score: 'x' }", "{ score: 'y' }", "{ score: 2 }"]).await);
		// `score * 2` fails with an arithmetic type error on the string rows.
		// Batch evaluation gives up on the whole column, and the per-row retry
		// resolves each ignorable failure to NONE — so both string rows land in
		// one NONE group while the numeric row keeps its own.
		let op = aggregate_over(
			input,
			&["score * 2"],
			vec![key("doubled", 0), count_star("total", &ctx).await],
			&ctx,
		)
		.await;

		let out = collect(&op, &ctx).await;
		assert_eq!(out.len(), 2);
		assert_eq!(field(&out[0], "doubled"), Value::None);
		assert_eq!(field(&out[0], "total"), Value::from(2));
		assert_eq!(field(&out[1], "doubled"), Value::from(4));
		assert_eq!(field(&out[1], "total"), Value::from(1));
	}

	#[tokio::test]
	async fn an_ignorable_error_in_an_aggregate_argument_accumulates_as_none() {
		let ctx = root_ctx();
		let input = ValuesOperator::new(rows(&["{ score: 'x' }", "{ score: 3 }"]).await);
		let op = aggregate_over(
			input,
			&[],
			vec![
				count_star("rows", &ctx).await,
				agg_of("sum", "math::sum", "score * 2", &ctx).await,
			],
			&ctx,
		)
		.await;

		// The failing row contributes NONE, which the sum accumulator skips; the
		// row itself still counts.
		assert_eq!(collect(&op, &ctx).await, rows(&["{ rows: 2, sum: 6 }"]).await);
	}

	#[tokio::test]
	async fn cancellation_between_batches_fails_the_stream() {
		let ctx = root_ctx();
		ctx.cancellation().cancel();
		let input = ValuesOperator::new(rows(&["{ c: 'us' }"]).await);
		let op =
			aggregate_over(input, &["c"], vec![key("c", 0), count_star("total", &ctx).await], &ctx)
				.await;

		let err = try_collect(&op, &ctx).await.expect_err("cancellation is reported");
		let ControlFlow::Err(err) = err else {
			panic!("cancellation surfaces as an error, not a control-flow signal");
		};
		assert!(crate::err::is_query_cancelled(&err), "unexpected error: {err}");
	}

	// =========================================================================
	// Planner-visible metadata
	// =========================================================================

	#[tokio::test]
	async fn required_context_lifts_to_database_when_a_key_or_argument_reads_a_field() {
		let ctx = root_ctx();

		// GROUP ALL count() reads no field, so Root is enough — the executor
		// validates this level before it calls execute().
		let op = aggregate_over(
			ValuesOperator::new(vec![]),
			&[],
			vec![count_star("total", &ctx).await],
			&ctx,
		)
		.await;
		assert_eq!(op.required_context(), ContextLevel::Root);

		// Field access may have to dereference a record id, which needs the
		// database context, so grouping on a field lifts the requirement.
		let key_expr = physical_expr("c", &ctx).await;
		assert_eq!(key_expr.required_context(), ContextLevel::Database);
		let op = aggregate_over(
			ValuesOperator::new(vec![]),
			&["c"],
			vec![key("c", 0), count_star("total", &ctx).await],
			&ctx,
		)
		.await;
		assert_eq!(op.required_context(), ContextLevel::Database);

		// An aggregate argument that reads a field lifts it just the same.
		let op = aggregate_over(
			ValuesOperator::new(vec![]),
			&[],
			vec![agg_of("sum", "math::sum", "score", &ctx).await],
			&ctx,
		)
		.await;
		assert_eq!(op.required_context(), ContextLevel::Database);
	}

	#[tokio::test]
	async fn access_mode_promotes_to_read_write_from_the_input_or_from_any_expression() {
		let ctx = root_ctx();

		let read_only = aggregate_over(
			ValuesOperator::new(vec![]),
			&["c"],
			vec![key("c", 0), count_star("total", &ctx).await],
			&ctx,
		)
		.await;
		assert_eq!(read_only.access_mode(), AccessMode::ReadOnly);

		// A mutating input makes the whole aggregate read-write, which is what
		// decides the transaction mode and the dependency-ordering barriers.
		let from_input = aggregate_over(
			ScriptedSource::read_write(vec![]),
			&["c"],
			vec![key("c", 0), count_star("total", &ctx).await],
			&ctx,
		)
		.await;
		assert_eq!(from_input.access_mode(), AccessMode::ReadWrite);

		// So does a mutating expression, wherever it sits.
		let writer = physical_expr("eval::surql('RETURN 1')", &ctx).await;
		assert_eq!(writer.access_mode(), AccessMode::ReadWrite);

		let from_key = aggregate_over(
			ValuesOperator::new(vec![]),
			&["eval::surql('RETURN 1')"],
			vec![count_star("total", &ctx).await],
			&ctx,
		)
		.await;
		assert_eq!(from_key.access_mode(), AccessMode::ReadWrite);

		let from_arg = aggregate_over(
			ValuesOperator::new(vec![]),
			&[],
			vec![agg_of("sum", "math::sum", "eval::surql('RETURN 1')", &ctx).await],
			&ctx,
		)
		.await;
		assert_eq!(from_arg.access_mode(), AccessMode::ReadWrite);

		let from_extra = aggregate_over(
			ValuesOperator::new(vec![]),
			&[],
			vec![single(
				"joined",
				ExtractedAggregate {
					function: aggregate_fn("array::join", &ctx),
					argument_expr: physical_expr("w", &ctx).await,
					extra_args: vec![physical_expr("eval::surql('RETURN 1')", &ctx).await],
				},
			)],
			&ctx,
		)
		.await;
		assert_eq!(from_extra.access_mode(), AccessMode::ReadWrite);

		let from_post = aggregate_over(
			ValuesOperator::new(vec![]),
			&[],
			vec![AggregateField::new(
				"mixed".to_string(),
				false,
				None,
				Some(AggregateExprInfo {
					aggregates: vec![ExtractedAggregate {
						function: aggregate_fn("math::sum", &ctx),
						argument_expr: physical_expr("score", &ctx).await,
						extra_args: vec![],
					}],
					post_expr: Some(physical_expr("eval::surql('RETURN 1')", &ctx).await),
				}),
				None,
			)],
			&ctx,
		)
		.await;
		assert_eq!(from_post.access_mode(), AccessMode::ReadWrite);

		let from_fallback = aggregate_over(
			ValuesOperator::new(vec![]),
			&[],
			vec![first_value("label", "eval::surql('RETURN 1')", &ctx).await],
			&ctx,
		)
		.await;
		assert_eq!(from_fallback.access_mode(), AccessMode::ReadWrite);
	}

	#[tokio::test]
	async fn output_ordering_is_unordered_so_a_sort_after_grouping_is_never_eliminated() {
		let ctx = root_ctx();
		let op = aggregate_over(
			ValuesOperator::new(rows(&["{ c: 'us' }"]).await),
			&["c"],
			vec![key("c", 0), count_star("total", &ctx).await],
			&ctx,
		)
		.await;

		// Groups leave the operator sorted by group key, but that is not
		// advertised. `Planner::can_eliminate_sort` reads exactly these two
		// methods, and both deny elimination, so `ORDER BY c` after `GROUP BY c`
		// always keeps its Sort operator.
		assert_eq!(op.output_ordering(), OutputOrdering::Unordered);
		assert!(op.constant_output_fields().is_empty());
		let required = vec![SortProperty {
			path: FieldPath::field("c"),
			direction: SortDirection::Asc,
			collate: false,
			numeric: false,
		}];
		assert!(!op.output_ordering().satisfies(&required));
	}

	#[tokio::test]
	async fn cardinality_hint_stays_unbounded_even_for_group_all() {
		let ctx = root_ctx();
		// `buffer_stream` picks a consumer's buffering strategy from this hint.
		// GROUP ALL emits exactly one row yet still reports Unbounded, so the
		// consumer buffers it through a spawned task rather than inline.
		let group_all = aggregate_over(
			ValuesOperator::new(vec![]),
			&[],
			vec![count_star("total", &ctx).await],
			&ctx,
		)
		.await;
		assert_eq!(group_all.cardinality_hint(), CardinalityHint::Unbounded);

		let grouped = aggregate_over(
			ValuesOperator::new(vec![]),
			&["c"],
			vec![key("c", 0), count_star("total", &ctx).await],
			&ctx,
		)
		.await;
		assert_eq!(grouped.cardinality_hint(), CardinalityHint::Unbounded);
	}

	#[tokio::test]
	async fn attrs_report_group_all_only_when_there_are_no_keys() {
		let ctx = root_ctx();
		let group_all = aggregate_over(
			ValuesOperator::new(vec![]),
			&[],
			vec![count_star("total", &ctx).await],
			&ctx,
		)
		.await;
		assert_eq!(group_all.attrs(), vec![("mode".to_string(), "GROUP ALL".to_string())]);

		let grouped = aggregate_over(
			ValuesOperator::new(vec![]),
			&["c", "y"],
			vec![key("c", 0), key("y", 1)],
			&ctx,
		)
		.await;
		assert_eq!(grouped.attrs(), vec![("by".to_string(), "c, y".to_string())]);
	}

	#[tokio::test]
	async fn expressions_expose_every_sub_expression_for_explain() {
		let ctx = root_ctx();
		let combined = AggregateField::new(
			"mixed".to_string(),
			false,
			None,
			Some(AggregateExprInfo {
				aggregates: vec![ExtractedAggregate {
					function: aggregate_fn("array::join", &ctx),
					argument_expr: physical_expr("w", &ctx).await,
					extra_args: vec![physical_expr("'-'", &ctx).await],
				}],
				post_expr: Some(physical_expr("_a0", &ctx).await),
			}),
			None,
		);
		let op = aggregate_over(
			ValuesOperator::new(vec![]),
			&["c"],
			vec![combined, first_value("label", "label", &ctx).await],
			&ctx,
		)
		.await;

		// EXPLAIN walks children + expressions; every compiled expression the
		// operator holds must be reachable, under a label naming its role.
		assert_eq!(op.children().len(), 1);
		let labels: Vec<&str> = op.expressions().into_iter().map(|(label, _)| label).collect();
		assert_eq!(
			labels,
			vec!["group_by", "agg_arg", "agg_extra", "agg_post_expr", "agg_fallback"]
		);
	}

	// =========================================================================
	// set_nested_value
	// =========================================================================

	#[test]
	fn set_nested_value_creates_missing_levels_and_replaces_non_objects() {
		let mut obj = Object::default();
		set_nested_value(&mut obj, &["a".to_string(), "b".to_string()], Value::from(1));
		assert_eq!(
			obj.get("a").cloned(),
			Some(Value::Object(Object::from_iter([("b".to_string(), Value::from(1))])))
		);

		// A scalar already sitting at a prefix of a later path is replaced by the
		// object that path needs.
		let mut obj = Object::default();
		set_nested_value(&mut obj, &["a".to_string()], Value::from(7));
		set_nested_value(&mut obj, &["a".to_string(), "b".to_string()], Value::from(1));
		assert_eq!(
			obj.get("a").cloned(),
			Some(Value::Object(Object::from_iter([("b".to_string(), Value::from(1))])))
		);

		// An empty path is a no-op.
		let mut obj = Object::default();
		set_nested_value(&mut obj, &[], Value::from(1));
		assert!(obj.is_empty());
	}

	// =========================================================================
	// GroupMap — group identity and ordering
	// =========================================================================

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
