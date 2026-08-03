//! LET operator - binds a value to a parameter name.
//!
//! LET is a context-mutating operator that adds a new parameter binding
//! to the execution context.

use std::sync::Arc;

use futures::stream;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::plan_or_compute::collect_stream;
use crate::exec::{
	AccessMode, BoxFut, CardinalityHint, Error as ExecError, ExecOperator, FlowResult,
	OperatorMetrics, ValueBatchStream, buffer_stream,
};
use crate::expr::{ControlFlow, Kind};
use crate::val::{Array, Value};

/// LET operator - binds a value to a parameter.
///
/// Implements `OperatorPlan` with `mutates_context() = true`.
/// The `output_context()` method evaluates the value and adds it to the
/// context parameters.
///
/// The value can be:
/// - A scalar expression (wrapped in `ExprPlan`) - evaluates to a single value
/// - A query - results are collected into an array
#[derive(Debug)]
pub struct LetPlan {
	/// Parameter name to bind (without $)
	pub name: Strand,
	/// Optional declared type for the binding — when present, the computed
	/// value is coerced to this kind before binding (mirrors
	/// `SetStatement::compute`).
	pub kind: Option<Kind>,
	/// Metrics for EXPLAIN ANALYZE
	pub(crate) metrics: Arc<OperatorMetrics>,
	/// Value to bind - either an ExprPlan for scalars or a query plan
	pub value: Arc<dyn ExecOperator>,
}

impl LetPlan {
	pub(crate) fn new(name: Strand, kind: Option<Kind>, value: Arc<dyn ExecOperator>) -> Self {
		Self {
			name,
			kind,
			value,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	fn coerce(&self, value: Value) -> Result<Value, Error> {
		match &self.kind {
			Some(kind) => value.coerce_to_kind(kind).map_err(|e| {
				ExecError::SetCoerce {
					name: self.name.to_string(),
					error: Box::new(e),
				}
				.into()
			}),
			None => Ok(value),
		}
	}

	/// Run the value plan and reduce its output to the single value this LET
	/// binds, before coercion.
	///
	/// A scalar value plan emits exactly one row, which becomes the bound value;
	/// an empty stream binds `NONE`. A query value plan binds all of its rows as
	/// an array.
	///
	/// Control-flow signals raised by the value plan are returned to the caller
	/// rather than resolved here, and reach it identically whether the plan
	/// raised them while building its stream or while draining it.
	async fn compute_value(&self, input: &ExecutionContext) -> crate::expr::FlowResult<Value> {
		let stream = buffer_stream(
			self.value.execute(input)?,
			self.value.access_mode(),
			self.value.cardinality_hint(),
			input.root().ctx.config.exec.operator_buffer_size,
		);
		let results = collect_stream(stream).await?;

		Ok(if self.value.is_scalar() {
			results.into_iter().next().unwrap_or(Value::None)
		} else {
			Value::Array(Array(results))
		})
	}
}
impl ExecOperator for LetPlan {
	fn name(&self) -> &'static str {
		"Let"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("name".to_string(), format!("${}", self.name.as_str()))]
	}

	fn required_context(&self) -> ContextLevel {
		self.value.required_context()
	}

	fn access_mode(&self) -> AccessMode {
		self.value.access_mode()
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
	}

	fn execute(&self, _ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		// LET returns NONE as its result (the binding happens in output_context)
		Ok(Box::pin(stream::once(async {
			Ok(crate::exec::ValueBatch {
				values: vec![Value::None],
			})
		})))
	}

	fn mutates_context(&self) -> bool {
		true
	}

	fn output_context<'a>(
		&'a self,
		input: &'a ExecutionContext,
	) -> BoxFut<'a, crate::expr::FlowResult<ExecutionContext>> {
		Box::pin(async move {
			// A RETURN from the value plan supplies the value to bind, and does so
			// whether it was raised as the plan started or partway through
			// producing rows.
			//
			// Everything else travels onwards unchanged: BREAK and CONTINUE belong
			// to an enclosing loop, so a LET in a FOR body breaks or continues that
			// loop rather than failing at the binding, and a stray one is rejected
			// by the statement boundary that has no loop to offer it. Errors reach
			// the boundary with their type intact; see the trait's
			// `output_context`.
			let computed_value = match self.compute_value(input).await {
				Ok(v) => v,
				Err(ControlFlow::Return(v)) => v,
				Err(ctrl) => return Err(ctrl),
			};

			// Apply declared type coercion (mirrors `SetStatement::compute`).
			let coerced = self.coerce(computed_value).map_err(|e| ControlFlow::Err(e.into()))?;
			Ok(input.with_param(self.name.clone(), coerced))
		})
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.value]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}
}

impl ToSql for LetPlan {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("LET $");
		f.push_str(self.name.as_str());
		f.push_str(" = ");
		if self.value.is_scalar() {
			f.push_str("<expr>");
		} else {
			f.push_str("(<query>)");
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::root_ctx;
	use crate::exec::{OutputOrdering, ValueBatch};

	/// Which signal a [`StubValue`] raises once its rows are exhausted.
	///
	/// A discriminant rather than a [`ControlFlow`]: the error arm owns an
	/// `anyhow::Error`, so `ControlFlow` is neither `Copy` nor `Clone` and cannot
	/// be handed to a stream that may be polled more than once.
	#[derive(Debug, Clone, Copy)]
	enum Signal {
		Break,
		Continue,
		Return(i64),
	}

	impl Signal {
		fn build(self) -> ControlFlow {
			match self {
				Signal::Break => ControlFlow::Break,
				Signal::Continue => ControlFlow::Continue,
				Signal::Return(v) => ControlFlow::Return(Value::from(v)),
			}
		}
	}

	/// A stub value plan for [`LetPlan`], covering the two places a value plan
	/// can raise a control-flow signal.
	///
	/// `rows` are emitted as one batch, then `signal` is raised. With `eager`
	/// set, `execute()` returns the signal instead of a stream and `rows` never
	/// appear; otherwise the signal arrives as a later stream item. `scalar`
	/// drives `is_scalar()`, which decides whether `LetPlan` binds one value or
	/// an array.
	#[derive(Debug)]
	struct StubValue {
		rows: Vec<Value>,
		signal: Option<Signal>,
		eager: bool,
		scalar: bool,
	}

	impl StubValue {
		/// A plan that only yields `rows`, with no signal.
		fn rows(rows: Vec<Value>) -> Self {
			Self {
				rows,
				signal: None,
				eager: false,
				scalar: true,
			}
		}

		/// A plan whose `execute()` raises `signal` before building a stream.
		fn eager(signal: Signal) -> Self {
			Self {
				rows: Vec::new(),
				signal: Some(signal),
				eager: true,
				scalar: true,
			}
		}

		/// A plan that emits `rows`, then raises `signal` from the stream.
		fn rows_then(rows: Vec<Value>, signal: Signal) -> Self {
			Self {
				rows,
				signal: Some(signal),
				eager: false,
				scalar: true,
			}
		}

		fn non_scalar(mut self) -> Self {
			self.scalar = false;
			self
		}

		fn into_operator(self) -> Arc<dyn ExecOperator> {
			Arc::new(self)
		}
	}

	impl ExecOperator for StubValue {
		fn name(&self) -> &'static str {
			"StubValue"
		}

		fn required_context(&self) -> ContextLevel {
			ContextLevel::Root
		}

		fn access_mode(&self) -> AccessMode {
			AccessMode::ReadOnly
		}

		fn cardinality_hint(&self) -> CardinalityHint {
			CardinalityHint::Unbounded
		}

		fn output_ordering(&self) -> OutputOrdering {
			OutputOrdering::Unordered
		}

		fn is_scalar(&self) -> bool {
			self.scalar
		}

		fn execute(&self, _ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
			if let Some(signal) = self.signal.filter(|_| self.eager) {
				return Err(signal.build());
			}

			let mut items: Vec<FlowResult<ValueBatch>> = Vec::new();
			if !self.rows.is_empty() {
				items.push(Ok(ValueBatch {
					values: self.rows.clone(),
				}));
			}
			if let Some(signal) = self.signal {
				items.push(Err(signal.build()));
			}

			Ok(Box::pin(stream::iter(items)))
		}
	}

	/// Build a `LET $x = <value>` plan with no declared type.
	fn let_plan(value: Arc<dyn ExecOperator>) -> LetPlan {
		LetPlan::new(Strand::new("x"), None, value)
	}

	/// Run `output_context` and read back the value bound to `$x`.
	async fn bound_value(plan: &LetPlan) -> crate::expr::FlowResult<Value> {
		let ctx = root_ctx();
		let out = plan.output_context(&ctx).await?;
		Ok(out.value("x").cloned().unwrap_or(Value::None))
	}

	/// Run `output_context` and return the signal it passed on, panicking if it
	/// bound a value instead.
	async fn propagated_signal(plan: &LetPlan) -> ControlFlow {
		match bound_value(plan).await {
			Ok(v) => panic!("expected a control-flow signal, got the binding: {v:?}"),
			Err(ctrl) => ctrl,
		}
	}

	#[tokio::test]
	async fn a_scalar_value_plan_binds_its_single_row() {
		let plan = let_plan(StubValue::rows(vec![Value::from(7)]).into_operator());
		assert_eq!(bound_value(&plan).await.unwrap(), Value::from(7));
	}

	#[tokio::test]
	async fn an_empty_scalar_value_plan_binds_none() {
		let plan = let_plan(StubValue::rows(Vec::new()).into_operator());
		assert_eq!(bound_value(&plan).await.unwrap(), Value::None);
	}

	#[tokio::test]
	async fn a_non_scalar_value_plan_binds_all_rows_as_an_array() {
		let plan = let_plan(
			StubValue::rows(vec![Value::from(1), Value::from(2)]).non_scalar().into_operator(),
		);
		assert_eq!(
			bound_value(&plan).await.unwrap(),
			Value::Array(Array(vec![Value::from(1), Value::from(2)]))
		);
	}

	#[tokio::test]
	async fn a_loop_signal_raised_by_the_value_plans_execute_travels_onwards() {
		let plan = let_plan(StubValue::eager(Signal::Break).into_operator());
		assert!(matches!(propagated_signal(&plan).await, ControlFlow::Break));

		let plan = let_plan(StubValue::eager(Signal::Continue).into_operator());
		assert!(matches!(propagated_signal(&plan).await, ControlFlow::Continue));
	}

	#[tokio::test]
	async fn a_loop_signal_from_inside_the_value_stream_travels_onwards_too() {
		// The signal arrives after a row has already been collected. Dropping it
		// here is what let a `BREAK` inside a LET value leave the enclosing loop
		// running, with a plausible-looking binding in place of the signal.
		let rows = vec![Value::from(1)];

		let plan = let_plan(StubValue::rows_then(rows.clone(), Signal::Break).into_operator());
		assert!(matches!(propagated_signal(&plan).await, ControlFlow::Break));

		let plan = let_plan(StubValue::rows_then(rows, Signal::Continue).into_operator());
		assert!(matches!(propagated_signal(&plan).await, ControlFlow::Continue));
	}

	#[tokio::test]
	async fn a_return_raised_by_the_value_plans_execute_supplies_the_bound_value() {
		let plan = let_plan(StubValue::eager(Signal::Return(9)).into_operator());
		assert_eq!(bound_value(&plan).await.unwrap(), Value::from(9));
	}

	#[tokio::test]
	async fn a_return_from_inside_the_value_stream_wins_over_the_rows_before_it() {
		let plan = let_plan(
			StubValue::rows_then(vec![Value::from(1), Value::from(2)], Signal::Return(9))
				.into_operator(),
		);
		assert_eq!(bound_value(&plan).await.unwrap(), Value::from(9));
	}

	#[tokio::test]
	async fn a_declared_type_coerces_the_bound_value() {
		let plan = LetPlan::new(
			Strand::new("x"),
			Some(Kind::String),
			StubValue::rows(vec![Value::from(7)]).into_operator(),
		);
		let err = match bound_value(&plan).await {
			Err(ControlFlow::Err(e)) => e,
			other => panic!("expected a coercion failure, got: {other:?}"),
		};
		assert!(
			format!("{err}").contains("$x"),
			"coercion failure should name the parameter, got: {err}"
		);
	}
}
