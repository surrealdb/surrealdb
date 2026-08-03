//! `Distinct` — whole-row dedup for `RETURN DISTINCT`.
//!
//! Sits above `Project` in the DISTINCT pipeline (`Project → Distinct →
//! Sort(columns) → Limit`, `doc/gql/V2_DESIGN.md` §5). It emits the first
//! occurrence of each distinct projected row and drops later duplicates,
//! preserving the input stream order so the downstream `Sort` orders only over
//! the returned columns (R7).
//!
//! Dedup hashes each row, probes its bucket, and compares candidates with
//! `PartialEq`. That treats two rows as distinct when they hash apart but compare
//! equal, which `Value` allows for numbers — `0.1f` and `0.1dec` are one value to
//! `=` and two rows here (see [`Value::hash_agrees_with_eq`]); the `Aggregate`
//! operator keys its group map on `Ord` for exactly that reason. The seen set
//! grows with the number of *distinct*
//! rows; its size is bounded by `SURREAL_GQL_MAX_JOIN_BUILD_ROWS` (the shared
//! GQL in-memory-build budget), and exceeding it fails the query with an error
//! that names the knob. Spill to disk is a future change, matching the
//! `Aggregate` stance.

// The GQL v2 MATCH operators are constructed only by the gql-gated
// planner (`Expr::Match` is `#[cfg(feature = "gql")]`), so they are dead
// code when the feature is off — suppress the lint there only, keeping
// dead-code detection active in the default (gql-on) build.
#![cfg_attr(not(feature = "gql"), allow(dead_code))]

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
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
				for value in batch.values {
					if seen.insert(&value, max_rows)? {
						values.push(value);
					}
				}
				if !values.is_empty() {
					yielder
						.emit(ValueBatch {
							values,
						})
						.await;
				}
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "Distinct", &self.metrics))
	}
}

/// Hash-keyed set of seen rows: each bucket is a `Vec` of rows that share a hash,
/// and membership is decided by `PartialEq` over that bucket (linear probe). Only
/// distinct rows are retained, so the total stored count is bounded by the
/// configured build-row budget.
///
/// The bucket step means membership is `Hash`-then-`PartialEq`, which is a
/// narrower relation than `PartialEq` alone for number-bearing rows — see the
/// module docs.
struct SeenSet {
	buckets: HashMap<u64, Vec<Value>>,
	len: usize,
}

impl SeenSet {
	fn new() -> Self {
		Self {
			buckets: HashMap::new(),
			len: 0,
		}
	}

	/// Record `value` if not already present. Returns `Ok(true)` when the value
	/// is newly inserted (caller should emit it), `Ok(false)` when it is a
	/// duplicate (caller should drop it). Fails when inserting would exceed
	/// `max_rows`.
	fn insert(&mut self, value: &Value, max_rows: usize) -> Result<bool, ControlFlow> {
		let hash = hash_value(value);
		let bucket = self.buckets.entry(hash).or_default();
		if bucket.iter().any(|seen| seen == value) {
			return Ok(false);
		}
		if self.len >= max_rows {
			return Err(ControlFlow::Err(anyhow::anyhow!(crate::exec::Error::InvalidStatement(
				format!(
					"GQL MATCH RETURN DISTINCT exceeded the maximum of {max_rows} distinct rows \
					 (configurable via SURREAL_GQL_MAX_JOIN_BUILD_ROWS)"
				),
			))));
		}
		bucket.push(value.clone());
		self.len += 1;
		Ok(true)
	}
}

/// Hash a single [`Value`] into a `u64` for bucket lookup. Deterministic within
/// a process (`DefaultHasher` uses a fixed seed).
fn hash_value(value: &Value) -> u64 {
	let mut hasher = DefaultHasher::new();
	value.hash(&mut hasher);
	hasher.finish()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::{ValuesOperator, collect, root_ctx};

	fn rows(ns: &[i64]) -> Vec<Value> {
		ns.iter().map(|n| Value::from(*n)).collect()
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
