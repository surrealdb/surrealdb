//! Optional chaining part -- `.?`.
//!
//! Instead of being a passive marker, `OptionalChainPart` wraps the remaining
//! tail of the idiom chain and handles short-circuit internally. If the input
//! is None/Null, the tail is skipped and None is returned immediately.

use std::sync::Arc;

use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, CombineAccessModes, ContextLevel};
use crate::expr::FlowResult;
use crate::val::Value;

/// Optional chaining - `.?` followed by remaining parts.
///
/// If the input value is None/Null, returns None immediately without
/// evaluating the tail. Otherwise evaluates the tail chain on the value.
///
/// For multiple optionals like `a.?.b.?.c`, the planner nests:
/// `[FieldPart("a"), OptionalChainPart { tail: [FieldPart("b"), OptionalChainPart { tail:
/// [FieldPart("c")] }] }]`
#[derive(Debug, Clone)]
pub struct OptionalChainPart {
	/// The remaining parts after the optional point.
	/// If the input is None/Null, these are skipped entirely.
	pub tail: Vec<Arc<dyn PhysicalExpr>>,
}
impl PhysicalExpr for OptionalChainPart {
	fn name(&self) -> &'static str {
		"OptionalChain"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		self.tail.iter().map(|p| p.required_context()).max().unwrap_or(ContextLevel::Root)
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let value = ctx.current_value.cloned().unwrap_or(Value::None);

			// Short-circuit on None/Null, preserving the original kind
			if matches!(value, Value::None | Value::Null) {
				return Ok(value);
			}

			// Evaluate the tail chain on the value
			let mut current = value;
			for part in &self.tail {
				current = part.evaluate(ctx.with_value(&current)).await?;
			}
			Ok(current)
		})
	}

	fn access_mode(&self) -> AccessMode {
		self.tail.iter().map(|p| p.access_mode()).combine_all()
	}
}

impl ToSql for OptionalChainPart {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push('?');
		for part in &self.tail {
			part.fmt_sql(f, fmt);
		}
	}
}
