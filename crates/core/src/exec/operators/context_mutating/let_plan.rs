//! LET operator - binds a value to a parameter name.
//!
//! LET is a context-mutating operator that adds a new parameter binding
//! to the execution context.

use std::sync::Arc;

use async_trait::async_trait;
use futures::{StreamExt, stream};
use surrealdb_types::{SqlFormat, ToSql};

use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, OperatorPlan, ValueBatchStream};
use crate::val::{Array, Value};

/// The value to bind in a LET statement.
#[derive(Debug, Clone)]
pub enum LetValue {
	/// Scalar expression - evaluates to exactly one Value
	/// Example: LET $x = 1 + 2
	Scalar(Arc<dyn PhysicalExpr>),

	/// Query - stream is collected into Value::Array
	/// Example: LET $users = SELECT * FROM user
	Query(Arc<dyn OperatorPlan>),
}

/// LET operator - binds a value to a parameter.
///
/// Implements `OperatorPlan` with `mutates_context() = true`.
/// The `output_context()` method evaluates the value (scalar or query)
/// and adds it to the context parameters.
#[derive(Debug)]
pub struct LetPlan {
	/// Parameter name to bind (without $)
	pub name: String,
	/// Value to bind
	pub value: LetValue,
}

#[async_trait]
impl OperatorPlan for LetPlan {
	fn name(&self) -> &'static str {
		"Let"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("name".to_string(), format!("${}", self.name))]
	}

	fn required_context(&self) -> ContextLevel {
		match &self.value {
			LetValue::Scalar(expr) => {
				if expr.references_current_value() {
					// This would be an error - LET can't reference row context
					// But we return Database as a safe fallback
					ContextLevel::Database
				} else {
					ContextLevel::Root
				}
			}
			LetValue::Query(plan) => plan.required_context(),
		}
	}

	fn access_mode(&self) -> AccessMode {
		// LET's access mode depends on its value expression
		match &self.value {
			LetValue::Scalar(expr) => expr.access_mode(),
			LetValue::Query(plan) => plan.access_mode(),
		}
	}

	fn execute(&self, _ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
		// LET produces no data output - it only mutates context
		Ok(Box::pin(stream::empty()))
	}

	fn mutates_context(&self) -> bool {
		true
	}

	async fn output_context(&self, input: &ExecutionContext) -> Result<ExecutionContext, Error> {
		let eval_ctx = EvalContext::from_exec_ctx(input);

		let computed_value = match &self.value {
			LetValue::Scalar(expr) => {
				expr.evaluate(eval_ctx).await.map_err(|e| Error::Thrown(e.to_string()))?
			}
			LetValue::Query(plan) => {
				// Execute the query and collect results into an array
				let stream = plan.execute(input)?;
				let results =
					collect_stream(stream).await.map_err(|e| Error::Thrown(e.to_string()))?;
				Value::Array(Array(results))
			}
		};

		// Add the parameter to the context
		Ok(input.with_param(self.name.clone(), computed_value))
	}

	fn children(&self) -> Vec<&Arc<dyn OperatorPlan>> {
		match &self.value {
			LetValue::Scalar(_) => vec![],
			LetValue::Query(plan) => vec![plan],
		}
	}
}

/// Collect all values from a stream into a Vec
async fn collect_stream(stream: ValueBatchStream) -> anyhow::Result<Vec<Value>> {
	let mut results = Vec::new();
	futures::pin_mut!(stream);

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
		f.push_str(&self.name);
		f.push_str(" = ");
		match &self.value {
			LetValue::Scalar(_) => f.push_str("<expr>"),
			LetValue::Query(_) => f.push_str("(<query>)"),
		}
	}
}

impl ToSql for LetValue {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		match self {
			Self::Scalar(_) => f.push_str("<expr>"),
			Self::Query(_) => f.push_str("(<query>)"),
		}
	}
}
