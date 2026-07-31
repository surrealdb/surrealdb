//! LET operator - binds a value to a parameter name.
//!
//! LET is a context-mutating operator that adds a new parameter binding
//! to the execution context.

use std::sync::Arc;

use futures::{StreamExt, stream};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::{
	AccessMode, BoxFut, CardinalityHint, Error as ExecError, ExecOperator, FlowResult,
	OperatorMetrics, ValueBatchStream, buffer_stream,
};
use crate::expr::Kind;
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
	) -> BoxFut<'a, anyhow::Result<ExecutionContext>> {
		Box::pin(async move {
			// Execute the value plan and collect results
			// Handle control flow signals explicitly
			let stream = match self.value.execute(input) {
				Ok(s) => buffer_stream(
					s,
					self.value.access_mode(),
					self.value.cardinality_hint(),
					input.root().ctx.config.exec.operator_buffer_size,
				),
				Err(crate::expr::ControlFlow::Return(v)) => {
					// If value expression returns early, use that value
					let coerced = self.coerce(v)?;
					return Ok(input.with_param(self.name.clone(), coerced));
				}
				Err(crate::expr::ControlFlow::Break | crate::expr::ControlFlow::Continue) => {
					return Err(Error::Exec(ExecError::InvalidControlFlow).into());
				}
				// Propagated unchanged; see the trait's `output_context`.
				Err(crate::expr::ControlFlow::Err(e)) => return Err(e),
			};
			let results = collect_stream(stream).await?;

			// If the value is a scalar expression, use the single result directly
			// Otherwise, wrap the results in an array
			let computed_value = if self.value.is_scalar() {
				// Scalar expressions return exactly one value
				results.into_iter().next().unwrap_or(Value::None)
			} else {
				// Queries return results as an array
				Value::Array(Array(results))
			};

			// Apply declared type coercion (mirrors `SetStatement::compute`).
			let coerced = self.coerce(computed_value)?;
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

/// Collect all values from a stream into a Vec
async fn collect_stream(mut stream: ValueBatchStream) -> anyhow::Result<Vec<Value>> {
	let mut results = Vec::new();

	while let Some(batch_result) = stream.next().await {
		match batch_result {
			Ok(batch) => results.extend(batch.values),
			Err(ctrl) => {
				use crate::expr::ControlFlow;
				match ctrl {
					ControlFlow::Break | ControlFlow::Continue => continue,
					ControlFlow::Return(v) => {
						results.push(v);
						break;
					}
					ControlFlow::Err(e) => {
						return Err(e);
					}
				}
			}
		}
	}

	Ok(results)
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
