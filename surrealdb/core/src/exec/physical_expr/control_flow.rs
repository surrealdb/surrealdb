//! Control flow physical expressions - BREAK, CONTINUE, THROW.
//!
//! These expressions signal control flow changes when evaluated inside blocks,
//! function bodies, or other expression contexts. They implement
//! [`PhysicalExpr`] so they can be used within expression-level planning
//! (e.g. inside a closure body or an `IF` branch). `RETURN` is a statement,
//! and in scalar position it is a subquery.

use std::sync::Arc;

use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, ContextLevel, Error as ExecError};
use crate::expr::{ControlFlow, FlowResult};
use crate::val::Value;

/// The kind of control flow operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlFlowKind {
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
			ControlFlowKind::Throw => write!(f, "THROW"),
			ControlFlowKind::Break => write!(f, "BREAK"),
			ControlFlowKind::Continue => write!(f, "CONTINUE"),
		}
	}
}

/// Control flow expression - BREAK, CONTINUE, THROW.
///
/// Produces a control flow signal rather than a value:
/// - BREAK/CONTINUE: signal immediately (no inner expression)
/// - THROW: evaluate inner expression, signal `ExecError::Thrown`
#[derive(Debug, Clone)]
pub struct ControlFlowExpr {
	/// The kind of control flow operation
	pub(crate) kind: ControlFlowKind,
	/// Inner expression for THROW (None for BREAK/CONTINUE)
	pub(crate) inner: Option<Arc<dyn PhysicalExpr>>,
}
impl PhysicalExpr for ControlFlowExpr {
	fn name(&self) -> &'static str {
		"ControlFlow"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		self.inner.as_ref().map_or(ContextLevel::Root, |e| e.required_context())
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			match self.kind {
				ControlFlowKind::Break => Err(ControlFlow::Break),
				ControlFlowKind::Continue => Err(ControlFlow::Continue),
				ControlFlowKind::Throw => {
					let inner = self.inner.as_ref().expect("THROW must have inner expression");
					let value = inner.evaluate(ctx).await?;
					Err(ControlFlow::Err(anyhow::Error::new(ExecError::Thrown(
						value.to_raw_string(),
					))))
				}
			}
		})
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
		}
	}
}
