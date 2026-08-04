//! Control flow operators - RETURN, THROW, BREAK, CONTINUE.
//!
//! These operators signal control flow changes to parent operators (blocks, loops).
//! They don't produce value streams in the normal sense - instead they return
//! control flow signals via `FlowResult`.

use std::sync::Arc;

use futures::StreamExt;

use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::{
	AccessMode, CardinalityHint, ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream,
	buffer_stream,
};
use crate::expr::ControlFlow;
use crate::val::Value;

/// Control flow operator - handles RETURN, THROW, BREAK, CONTINUE.
///
/// This operator signals control flow changes to parent operators.
/// - RETURN: Evaluates inner plan, returns `ControlFlow::Return(value)`
/// - THROW: Evaluates inner plan, returns `ControlFlow::Err(exec::Error::Thrown(...))`
/// - BREAK: Returns `ControlFlow::Break` immediately
/// - CONTINUE: Returns `ControlFlow::Continue` immediately
#[derive(Debug)]
pub struct ReturnPlan {
	pub inner: Arc<dyn ExecOperator>,
	/// Metrics for EXPLAIN ANALYZE
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl ReturnPlan {
	pub(crate) fn new(inner: Arc<dyn ExecOperator>) -> Self {
		Self {
			inner,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}
impl ExecOperator for ReturnPlan {
	fn name(&self) -> &'static str {
		"Return"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![]
	}

	fn required_context(&self) -> ContextLevel {
		self.inner.required_context()
	}

	fn access_mode(&self) -> AccessMode {
		self.inner.access_mode()
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		self.inner.cardinality_hint()
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let inner = Arc::clone(&self.inner);
		let ctx = ctx.clone();

		// Check if inner plan is scalar (like `RETURN 1 + 2`) vs query (like `RETURN SELECT
		// ...`) Query results should stay wrapped in array; scalar results can be
		// unwrapped
		let inner_is_scalar = inner.is_scalar();

		// Return a stream that executes the inner plan and produces the control flow signal
		Ok(Box::pin(futures::stream::once(async move {
			// Execute inner plan and collect values
			let mut stream = match inner.execute(&ctx) {
				Ok(s) => buffer_stream(
					s,
					inner.access_mode(),
					inner.cardinality_hint(),
					ctx.root().ctx.config.exec.operator_buffer_size,
				),
				Err(ctrl) => return Err(ctrl),
			};

			let mut values = Vec::new();
			while let Some(batch_result) = stream.next().await {
				match batch_result {
					Ok(batch) => values.extend(batch.values),
					Err(ControlFlow::Return(v)) => {
						values.push(v);
						break;
					}
					Err(e) => return Err(e),
				}
			}

			// Get the result value
			// For scalar expressions (like `RETURN 1 + 2`), unwrap single values
			// For query expressions (like `RETURN SELECT ...`), keep array wrapping
			let value = if inner_is_scalar {
				// Scalar: unwrap single value, use NONE for empty
				if values.len() == 1 {
					values.into_iter().next().expect("values verified non-empty")
				} else if values.is_empty() {
					Value::None
				} else {
					Value::Array(crate::val::Array(values))
				}
			} else {
				// Query: always wrap in array (matches SELECT behavior)
				Value::Array(crate::val::Array(values))
			};

			Err(ControlFlow::Return(value))
		})))
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.inner]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::ExprPlan;
	use crate::exec::operators::test_util::{ValuesOperator, physical_expr, root_ctx, try_collect};
	use crate::exec::{Error as ExecError, ValueBatch};
	use crate::val::Array;

	/// A control-flow signal a [`Stub`] raises. `ControlFlow` is not `Clone`, so
	/// the signal is described here and built fresh at each use.
	#[derive(Debug, Clone)]
	enum Signal {
		Return(Value),
		Break,
		Throw(&'static str),
	}

	impl Signal {
		fn build(&self) -> ControlFlow {
			match self {
				Signal::Return(v) => ControlFlow::Return(v.clone()),
				Signal::Break => ControlFlow::Break,
				Signal::Throw(msg) => {
					ControlFlow::Err(anyhow::Error::new(ExecError::Thrown((*msg).to_owned())))
				}
			}
		}
	}

	/// A scriptable inner plan, so a test can put `ReturnPlan` in front of every
	/// shape it distinguishes: scalar versus query, zero / one / many rows, and a
	/// control-flow signal raised either by `execute()` itself or from a chosen
	/// position inside the stream.
	///
	/// Each row is emitted as its own batch so a signal can be interleaved
	/// between rows.
	#[derive(Debug)]
	struct Stub {
		rows: Vec<Value>,
		scalar: bool,
		/// Signal raised by `execute()`, before any stream exists.
		execute_signal: Option<Signal>,
		/// Signal yielded by the stream in place of the row at this index.
		stream_signal: Option<(usize, Signal)>,
		access_mode: AccessMode,
		required_context: ContextLevel,
		cardinality: CardinalityHint,
	}

	impl Stub {
		fn new(rows: Vec<Value>) -> Self {
			Self {
				rows,
				scalar: false,
				execute_signal: None,
				stream_signal: None,
				access_mode: AccessMode::ReadOnly,
				required_context: ContextLevel::Root,
				cardinality: CardinalityHint::Unbounded,
			}
		}

		fn scalar(mut self) -> Self {
			self.scalar = true;
			self
		}

		fn signal_from_execute(mut self, signal: Signal) -> Self {
			self.execute_signal = Some(signal);
			self
		}

		fn signal_at(mut self, index: usize, signal: Signal) -> Self {
			self.stream_signal = Some((index, signal));
			self
		}

		fn metadata(
			mut self,
			access_mode: AccessMode,
			required_context: ContextLevel,
			cardinality: CardinalityHint,
		) -> Self {
			self.access_mode = access_mode;
			self.required_context = required_context;
			self.cardinality = cardinality;
			self
		}

		fn op(self) -> Arc<dyn ExecOperator> {
			Arc::new(self)
		}
	}

	impl ExecOperator for Stub {
		fn name(&self) -> &'static str {
			"Stub"
		}

		fn required_context(&self) -> ContextLevel {
			self.required_context
		}

		fn access_mode(&self) -> AccessMode {
			self.access_mode
		}

		fn cardinality_hint(&self) -> CardinalityHint {
			self.cardinality
		}

		fn is_scalar(&self) -> bool {
			self.scalar
		}

		fn execute(&self, _ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
			if let Some(signal) = &self.execute_signal {
				return Err(signal.build());
			}
			let mut items: Vec<FlowResult<ValueBatch>> = Vec::new();
			for (i, row) in self.rows.iter().enumerate() {
				if let Some((at, signal)) = &self.stream_signal
					&& *at == i
				{
					items.push(Err(signal.build()));
				}
				items.push(Ok(ValueBatch {
					values: vec![row.clone()],
				}));
			}
			if let Some((at, signal)) = &self.stream_signal
				&& *at >= self.rows.len()
			{
				items.push(Err(signal.build()));
			}
			Ok(Box::pin(futures::stream::iter(items)))
		}
	}

	/// Run `plan` and take the value it returned, asserting it signalled RETURN.
	async fn returned(plan: Arc<dyn ExecOperator>, ctx: &ExecutionContext) -> Value {
		match try_collect(&plan, ctx).await {
			Err(ControlFlow::Return(v)) => v,
			Err(other) => panic!("expected RETURN, got {other}"),
			Ok(rows) => panic!("RETURN never emits rows, got {rows:?}"),
		}
	}

	#[tokio::test]
	async fn a_scalar_inner_expression_is_surfaced_as_its_bare_value() {
		let ctx = root_ctx();
		let inner: Arc<dyn ExecOperator> =
			Arc::new(ExprPlan::new(physical_expr("1 + 2", &ctx).await));
		let plan: Arc<dyn ExecOperator> = Arc::new(ReturnPlan::new(inner));
		assert_eq!(returned(plan, &ctx).await, Value::from(3i64));
	}

	#[tokio::test]
	async fn a_scalar_inner_that_produces_no_row_returns_none() {
		let ctx = root_ctx();
		// This is the shape a valueless RETURN takes at operator level: nothing to
		// unwrap, so NONE stands in.
		let plan: Arc<dyn ExecOperator> =
			Arc::new(ReturnPlan::new(Stub::new(vec![]).scalar().op()));
		assert_eq!(returned(plan, &ctx).await, Value::None);
	}

	#[tokio::test]
	async fn a_scalar_inner_that_produces_several_rows_is_collected_into_an_array() {
		let ctx = root_ctx();
		let inner = Stub::new(vec![Value::from(1i64), Value::from(2i64)]).scalar().op();
		let plan: Arc<dyn ExecOperator> = Arc::new(ReturnPlan::new(inner));
		assert_eq!(
			returned(plan, &ctx).await,
			Value::Array(Array(vec![Value::from(1i64), Value::from(2i64)]))
		);
	}

	#[tokio::test]
	async fn a_query_inner_keeps_its_array_wrapping_at_every_row_count() {
		let ctx = root_ctx();

		// A single-row query result must not be unwrapped: `RETURN SELECT …` has
		// to keep looking like a SELECT result.
		let plan: Arc<dyn ExecOperator> =
			Arc::new(ReturnPlan::new(ValuesOperator::new(vec![Value::from(1i64)])));
		assert_eq!(returned(plan, &ctx).await, Value::Array(Array(vec![Value::from(1i64)])));

		let plan: Arc<dyn ExecOperator> = Arc::new(ReturnPlan::new(ValuesOperator::new(vec![])));
		assert_eq!(returned(plan, &ctx).await, Value::Array(Array(vec![])));
	}

	#[tokio::test]
	async fn a_return_from_inside_the_inner_stream_is_appended_and_ends_collection() {
		let ctx = root_ctx();
		// Rows 1 and 2 exist, but the signal sits in front of row 2: the returned
		// value joins the rows already collected and nothing after it is consumed.
		let inner = Stub::new(vec![Value::from(1i64), Value::from(2i64)])
			.signal_at(1, Signal::Return(Value::from(9i64)))
			.op();
		let plan: Arc<dyn ExecOperator> = Arc::new(ReturnPlan::new(inner));
		assert_eq!(
			returned(plan, &ctx).await,
			Value::Array(Array(vec![Value::from(1i64), Value::from(9i64)]))
		);
	}

	#[tokio::test]
	async fn a_break_from_inside_the_inner_stream_propagates_as_break() {
		let ctx = root_ctx();
		// RETURN must not swallow or reclassify a loop signal raised beneath it.
		let inner = Stub::new(vec![Value::from(1i64)]).signal_at(0, Signal::Break).op();
		let plan: Arc<dyn ExecOperator> = Arc::new(ReturnPlan::new(inner));
		let flow = try_collect(&plan, &ctx).await.expect_err("BREAK must propagate");
		assert!(matches!(flow, ControlFlow::Break), "got {flow}");
	}

	#[tokio::test]
	async fn an_error_from_inside_the_inner_stream_propagates_unchanged() {
		let ctx = root_ctx();
		let inner = Stub::new(vec![Value::from(1i64)]).signal_at(0, Signal::Throw("boom")).op();
		let plan: Arc<dyn ExecOperator> = Arc::new(ReturnPlan::new(inner));
		let flow = try_collect(&plan, &ctx).await.expect_err("the error must propagate");
		let ControlFlow::Err(e) = flow else {
			panic!("expected an error");
		};
		assert!(
			matches!(e.downcast_ref::<ExecError>(), Some(ExecError::Thrown(msg)) if msg == "boom"),
			"the inner error must reach the caller with its type intact, got {e:?}"
		);
	}

	#[tokio::test]
	async fn a_signal_raised_by_the_inner_execute_propagates_unchanged() {
		let ctx = root_ctx();
		// `execute()` is fallible before any stream exists; that signal is passed
		// through rather than turned into a RETURN.
		let inner = Stub::new(vec![]).signal_from_execute(Signal::Break).op();
		let plan: Arc<dyn ExecOperator> = Arc::new(ReturnPlan::new(inner));
		let flow = try_collect(&plan, &ctx).await.expect_err("BREAK must propagate");
		assert!(matches!(flow, ControlFlow::Break), "got {flow}");
	}

	#[tokio::test]
	async fn metadata_is_inherited_from_the_inner_plan() {
		// All three are load-bearing: `access_mode` selects the transaction type
		// (so `RETURN (CREATE …)` must report ReadWrite), `required_context` drives
		// the executor's pre-flight level check, and `cardinality_hint` selects the
		// buffering strategy `buffer_stream` applies.
		let inner = Stub::new(vec![])
			.metadata(AccessMode::ReadWrite, ContextLevel::Database, CardinalityHint::AtMostOne)
			.op();
		let plan = ReturnPlan::new(inner);
		assert_eq!(plan.access_mode(), AccessMode::ReadWrite);
		assert_eq!(plan.required_context(), ContextLevel::Database);
		assert_eq!(plan.cardinality_hint(), CardinalityHint::AtMostOne);
	}
}
