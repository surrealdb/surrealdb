//! `Distinct` — whole-row dedup for `RETURN DISTINCT`.
//!
//! Sits above `Project` in the DISTINCT pipeline (`Project → Distinct →
//! Sort(columns) → Limit`, `doc/gql/V2_DESIGN.md` §5). It emits the first
//! occurrence of each distinct projected row and drops later duplicates,
//! preserving the input stream order so the downstream `Sort` orders only over
//! the returned columns (R7).
//!
//! Two rows are the same row when they compare equal, so the seen set is
//! **ordered** (`Value: Ord`) rather than hashed. `Value: Hash` disagrees with
//! `Value: PartialEq` for numbers — `0.1f == 0.1dec` while the two hash apart, so
//! a hash-bucketed set would never compare them and would emit one value as two
//! rows (see [`Value::hash_agrees_with_eq`]). The `Aggregate` operator keys its
//! group map on `Ord` for the same reason.
//!
//! The set grows with the number of *distinct* rows; its size is bounded by
//! `SURREAL_GQL_MAX_JOIN_BUILD_ROWS` (the shared GQL in-memory-build budget), and
//! exceeding it fails the query with an error that names the knob. Spill to disk
//! is a future change, matching the `Aggregate` stance.

use std::collections::BTreeSet;
use std::sync::Arc;

use common::future::stream::{self, Yielder};
use futures::StreamExt;

use crate::exec::{
	AccessMode, ContextLevel, ExecOperator, ExecutionContext, FlowResult, OperatorMetrics,
	OutputOrdering, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::expr::ControlFlow;
use crate::val::Value;

/// Emits the first occurrence of each distinct input row, dropping duplicates
/// while preserving stream order.
#[derive(Debug, Clone)]
pub struct Distinct {
	pub(crate) input: Arc<dyn ExecOperator>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl Distinct {
	/// Create a new `Distinct` over `input`.
	pub(crate) fn new(input: Arc<dyn ExecOperator>) -> Self {
		Self {
			input,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

impl ExecOperator for Distinct {
	fn name(&self) -> &'static str {
		"Distinct"
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database.max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		self.input.access_mode()
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn output_ordering(&self) -> OutputOrdering {
		// Duplicates are removed but surviving rows keep their input order, so
		// any ordering the input guarantees still holds.
		self.input.output_ordering()
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let mut input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		let max_rows = ctx.root().ctx.config.exec.gql_max_join_build_rows;
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			let mut seen = SeenSet::new();
			while let Some(batch_result) = input_stream.next().await {
				crate::exec::operators::check_cancelled(&ctx)?;
				let batch = batch_result?;
				let mut values = Vec::new();
				for value in batch.into_values() {
					if seen.insert(&value, max_rows)? {
						values.push(value);
					}
				}
				if !values.is_empty() {
					yielder.emit(ValueBatch::new(values)).await;
				}
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "Distinct", &self.metrics))
	}
}

/// Set of seen rows, keyed by `Value: Ord` so membership is exactly the equality
/// the language uses — see the module docs for why a hash-bucketed set is not
/// equivalent. Only distinct rows are retained, so the stored count is bounded by
/// the configured build-row budget.
struct SeenSet {
	seen: BTreeSet<Value>,
}

impl SeenSet {
	fn new() -> Self {
		Self {
			seen: BTreeSet::new(),
		}
	}

	/// Record `value` if not already present. Returns `Ok(true)` when the value
	/// is newly inserted (caller should emit it), `Ok(false)` when it is a
	/// duplicate (caller should drop it). Fails when inserting would exceed
	/// `max_rows`.
	///
	/// The duplicate case — the common one for a dedup — probes without cloning;
	/// only a newly seen row is cloned into the set.
	fn insert(&mut self, value: &Value, max_rows: usize) -> Result<bool, ControlFlow> {
		if self.seen.contains(value) {
			return Ok(false);
		}
		if self.seen.len() >= max_rows {
			return Err(ControlFlow::Err(anyhow::anyhow!(crate::exec::Error::InvalidStatement(
				format!(
					"GQL MATCH RETURN DISTINCT exceeded the maximum of {max_rows} distinct rows \
					 (configurable via SURREAL_GQL_MAX_JOIN_BUILD_ROWS)"
				),
			))));
		}
		self.seen.insert(value.clone());
		Ok(true)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::{ValuesOperator, collect, root_ctx};

	fn rows(ns: &[i64]) -> Vec<Value> {
		ns.iter().map(|n| Value::from(*n)).collect()
	}

	fn dec(s: &str) -> Value {
		use common::decimal::DecimalExt;
		Value::Number(crate::val::Number::Decimal(
			rust_decimal::Decimal::from_str_normalized(s).unwrap(),
		))
	}

	fn float(f: f64) -> Value {
		Value::Number(crate::val::Number::Float(f))
	}

	/// Rows that compare equal are one row, whichever numeric variant carries
	/// them. `0.1f` and `0.1dec` compare equal but hash apart, so a hash-bucketed
	/// seen set would never compare them and would emit both.
	#[tokio::test]
	async fn numerically_equal_rows_dedup_to_one() {
		// Integral spellings hash alike, so they would collapse either way; the
		// non-integral pair is the one that needs the ordered set.
		let input = ValuesOperator::new(vec![
			float(0.1),
			dec("0.1"),
			Value::from(1i64),
			float(1.0),
			dec("1"),
			dec("1.0"),
			float(1.5),
			dec("1.50"),
		]);
		let distinct: Arc<dyn ExecOperator> = Arc::new(Distinct::new(input));

		let out = collect(&distinct, &root_ctx()).await;
		// One row per value, each the first spelling seen.
		assert_eq!(out, vec![float(0.1), Value::from(1i64), float(1.5)]);
	}

	/// Distinct numbers stay distinct — the ordered set must not over-collapse.
	#[tokio::test]
	async fn numerically_distinct_rows_are_kept() {
		let input =
			ValuesOperator::new(vec![float(0.1), dec("0.2"), float(0.10001), dec("0.1000000001")]);
		let distinct: Arc<dyn ExecOperator> = Arc::new(Distinct::new(input));

		let out = collect(&distinct, &root_ctx()).await;
		assert_eq!(out.len(), 4, "{out:?}");
	}

	#[tokio::test]
	async fn dedups_preserving_first_occurrence_order() {
		let input = ValuesOperator::new(rows(&[3, 1, 3, 2, 1, 2, 3]));
		let distinct: Arc<dyn ExecOperator> = Arc::new(Distinct::new(input));
		let ctx = root_ctx();

		let out = collect(&distinct, &ctx).await;
		// First occurrences only, in stream order: 3, 1, 2.
		assert_eq!(out, rows(&[3, 1, 2]));
	}

	#[tokio::test]
	async fn passes_distinct_rows_through_unchanged() {
		let input = ValuesOperator::new(rows(&[5, 4, 3, 2, 1]));
		let distinct: Arc<dyn ExecOperator> = Arc::new(Distinct::new(input));
		let ctx = root_ctx();
		assert_eq!(collect(&distinct, &ctx).await, rows(&[5, 4, 3, 2, 1]));
	}

	#[tokio::test]
	async fn empty_input_yields_nothing() {
		let input = ValuesOperator::new(Vec::new());
		let distinct: Arc<dyn ExecOperator> = Arc::new(Distinct::new(input));
		let ctx = root_ctx();
		assert!(collect(&distinct, &ctx).await.is_empty());
	}

	#[tokio::test]
	async fn dedups_structurally_equal_rows() {
		use crate::val::Object;
		let row = || {
			let mut o = Object::default();
			o.insert("a".to_string(), Value::from(1));
			Value::Object(o)
		};
		let input = ValuesOperator::new(vec![row(), row(), row()]);
		let distinct: Arc<dyn ExecOperator> = Arc::new(Distinct::new(input));
		let ctx = root_ctx();
		let out = collect(&distinct, &ctx).await;
		assert_eq!(out, vec![row()]);
	}

	#[test]
	fn seen_set_guard_names_the_knob() {
		let mut seen = SeenSet::new();
		// First insert under a budget of 1 succeeds.
		assert_eq!(seen.insert(&Value::from(1), 1).unwrap(), true);
		// Re-inserting the same value is a duplicate, not a budget failure.
		assert_eq!(seen.insert(&Value::from(1), 1).unwrap(), false);
		// A new distinct value past the budget errors and names the knob.
		let err = seen.insert(&Value::from(2), 1).unwrap_err();
		let msg = match err {
			ControlFlow::Err(e) => e.to_string(),
			other => panic!("expected error, got {other:?}"),
		};
		assert!(
			msg.contains("SURREAL_GQL_MAX_JOIN_BUILD_ROWS"),
			"guard error must name the knob, got: {msg}"
		);
	}

	#[test]
	fn distinct_reports_name() {
		let distinct = Distinct::new(ValuesOperator::new(Vec::new()));
		assert_eq!(distinct.name(), "Distinct");
	}
}
