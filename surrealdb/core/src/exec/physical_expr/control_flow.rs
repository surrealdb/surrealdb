//! Control flow physical expressions - BREAK, CONTINUE, THROW, RETURN.
//!
//! These expressions signal control flow changes when evaluated inside blocks,
//! function bodies, or other expression contexts. They mirror the behavior of
//! [`crate::exec::operators::control_flow::ControlFlowPlan`] but implement
//! [`PhysicalExpr`] so they can be used within expression-level planning
//! (e.g. inside `BlockPhysicalExpr`).

use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::err::Error;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, ContextLevel};
use crate::expr::{ControlFlow, FlowResult};
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

/// Control flow expression - BREAK, CONTINUE, THROW, RETURN.
///
/// Produces a control flow signal rather than a value:
/// - BREAK/CONTINUE: signal immediately (no inner expression)
/// - THROW: evaluate inner expression, signal `Error::Thrown`
/// - RETURN: evaluate inner expression, signal `ControlFlow::Return`
#[derive(Debug, Clone)]
pub struct ControlFlowExpr {
	/// The kind of control flow operation
	pub(crate) kind: ControlFlowKind,
	/// Inner expression for THROW/RETURN (None for BREAK/CONTINUE)
	pub(crate) inner: Option<Arc<dyn PhysicalExpr>>,
}

#[async_trait]
impl PhysicalExpr for ControlFlowExpr {
	fn name(&self) -> &'static str {
		"ControlFlow"
	}

	fn required_context(&self) -> ContextLevel {
		self.inner.as_ref().map_or(ContextLevel::Root, |e| e.required_context())
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		match self.kind {
			ControlFlowKind::Break => Err(ControlFlow::Break),
			ControlFlowKind::Continue => Err(ControlFlow::Continue),
			ControlFlowKind::Throw => {
				let inner = self.inner.as_ref().expect("THROW must have inner expression");
				let value = inner.evaluate(ctx).await?;
				Err(ControlFlow::Err(anyhow::Error::new(Error::Thrown(value.to_raw_string()))))
			}
			ControlFlowKind::Return => {
				let inner = self.inner.as_ref().expect("RETURN must have inner expression");
				let value = inner.evaluate(ctx).await?;
				Err(ControlFlow::Return(value))
			}
		}
	}

	fn references_current_value(&self) -> bool {
		self.inner.as_ref().is_some_and(|e| e.references_current_value())
	}

	fn access_mode(&self) -> AccessMode {
		self.inner.as_ref().map_or(AccessMode::ReadOnly, |e| e.access_mode())
	}
}

impl ToSql for ControlFlowExpr {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self.kind {
			ControlFlowKind::Break => f.push_str("BREAK"),
			ControlFlowKind::Continue => f.push_str("CONTINUE"),
			ControlFlowKind::Throw => {
				f.push_str("THROW");
				if let Some(inner) = &self.inner {
					write_sql!(f, fmt, " {}", inner);
				}
			}
			ControlFlowKind::Return => {
				f.push_str("RETURN");
				if let Some(inner) = &self.inner {
					write_sql!(f, fmt, " {}", inner);
				}
			}
		}
	}
}
