//! Control flow operators - RETURN, THROW, BREAK, CONTINUE.
//!
//! These operators signal control flow changes to parent operators (blocks, loops).
//! They don't produce value streams in the normal sense - instead they return
//! control flow signals via `FlowResult`.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use surrealdb_types::{SqlFormat, ToSql};

use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::{AccessMode, ExecOperator, FlowResult, ValueBatchStream};
use crate::expr::ControlFlow;
use crate::val::Value;

/// The kind of control flow operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlFlowKind {
	/// RETURN - exit from block/function with a value
	Return,
	/// THROW - raise an error
	Throw,
	/// BREAK - exit from loop
	Break,
	/// CONTINUE - skip to next loop iteration
	Continue,
}

impl std::fmt::Display for ControlFlowKind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ControlFlowKind::Return => write!(f, "RETURN"),
			ControlFlowKind::Throw => write!(f, "THROW"),
			ControlFlowKind::Break => write!(f, "BREAK"),
			ControlFlowKind::Continue => write!(f, "CONTINUE"),
		}
	}
}

/// Control flow operator - handles RETURN, THROW, BREAK, CONTINUE.
///
/// This operator signals control flow changes to parent operators.
/// - RETURN: Evaluates inner plan, returns `ControlFlow::Return(value)`
/// - THROW: Evaluates inner plan, returns `ControlFlow::Err(Error::Thrown(...))`
/// - BREAK: Returns `ControlFlow::Break` immediately
/// - CONTINUE: Returns `ControlFlow::Continue` immediately
#[derive(Debug)]
pub struct ControlFlowPlan {
	/// The kind of control flow operation
	pub kind: ControlFlowKind,
	/// Inner plan for RETURN/THROW (None for BREAK/CONTINUE)
	pub inner: Option<Arc<dyn ExecOperator>>,
}

#[async_trait]
impl ExecOperator for ControlFlowPlan {
	fn name(&self) -> &'static str {
		"ControlFlow"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("kind".to_string(), self.kind.to_string())]
	}

	fn required_context(&self) -> ContextLevel {
		// Delegate to inner plan if present, otherwise root is fine
		self.inner.as_ref().map(|p| p.required_context()).unwrap_or(ContextLevel::Root)
	}

	fn access_mode(&self) -> AccessMode {
		// Delegate to inner plan if present, otherwise read-only
		self.inner.as_ref().map(|p| p.access_mode()).unwrap_or(AccessMode::ReadOnly)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		match self.kind {
			// BREAK and CONTINUE return immediately - no value to compute
			ControlFlowKind::Break => Err(ControlFlow::Break),
			ControlFlowKind::Continue => Err(ControlFlow::Continue),

			// RETURN and THROW need to execute inner plan first
			ControlFlowKind::Return | ControlFlowKind::Throw => {
				let inner = self.inner.as_ref().expect("RETURN/THROW must have inner plan").clone();
				let kind = self.kind;
				let ctx = ctx.clone();

				// Return a stream that executes the inner plan and produces the control flow signal
				Ok(Box::pin(futures::stream::once(async move {
					// Execute inner plan and collect values
					let mut stream = match inner.execute(&ctx) {
						Ok(s) => s,
						Err(ctrl) => return Err(ctrl),
					};

					let mut values = Vec::new();
					while let Some(batch_result) = stream.next().await {
						match batch_result {
							Ok(batch) => values.extend(batch.values),
							Err(ControlFlow::Return(v)) => {
								// Nested return - propagate for RETURN, use value for THROW
								if kind == ControlFlowKind::Return {
									return Err(ControlFlow::Return(v));
								}
								values.push(v);
								break;
							}
							Err(e) => return Err(e),
						}
					}

					// Get the result value
					let value = if values.len() == 1 {
						values.into_iter().next().unwrap()
					} else if values.is_empty() {
						Value::None
					} else {
						Value::Array(crate::val::Array(values))
					};

					// Produce the appropriate control flow signal
					match kind {
						ControlFlowKind::Return => Err(ControlFlow::Return(value)),
						ControlFlowKind::Throw => Err(ControlFlow::Err(anyhow::Error::new(
							Error::Thrown(value.to_raw_string()),
						))),
						_ => unreachable!(),
					}
				})))
			}
		}
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		self.inner.as_ref().map(|p| vec![p]).unwrap_or_default()
	}
}

impl ToSql for ControlFlowPlan {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		match self.kind {
			ControlFlowKind::Return => {
				f.push_str("RETURN");
				if self.inner.is_some() {
					f.push_str(" <expr>");
				}
			}
			ControlFlowKind::Throw => {
				f.push_str("THROW");
				if self.inner.is_some() {
					f.push_str(" <expr>");
				}
			}
			ControlFlowKind::Break => f.push_str("BREAK"),
			ControlFlowKind::Continue => f.push_str("CONTINUE"),
		}
	}
}
