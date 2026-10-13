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

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::{eval_on, root_ctx, val};
	use crate::exec::parts::FieldPart;

	/// A tail of plain field accesses, as the planner builds for `.?.a.b`.
	fn field_tail(names: &[&str]) -> Vec<Arc<dyn PhysicalExpr>> {
		names
			.iter()
			.map(|name| {
				Arc::new(FieldPart {
					name: (*name).to_owned(),
				}) as Arc<dyn PhysicalExpr>
			})
			.collect()
	}

	#[tokio::test]
	async fn none_and_null_skip_the_tail_and_keep_their_own_kind() {
		let ctx = root_ctx();
		// NULL is the load-bearing case: the tail would turn it into NONE, so a
		// NULL result is proof the tail never ran.
		assert_eq!(eval_on("a.?.b", &val("{ a: NULL }").await, &ctx).await.unwrap(), Value::Null);
		assert_eq!(eval_on("a.?.b", &val("{ a: NONE }").await, &ctx).await.unwrap(), Value::None);
		// A missing field is NONE and short-circuits the same way.
		assert_eq!(eval_on("a.?.b", &val("{ }").await, &ctx).await.unwrap(), Value::None);
	}

	#[tokio::test]
	async fn a_present_value_is_threaded_through_the_whole_tail() {
		let ctx = root_ctx();
		let doc = val("{ a: { b: { c: 7 } } }").await;
		assert_eq!(eval_on("a.?.b.c", &doc, &ctx).await.unwrap(), Value::from(7));
		// The tail runs even when it produces NONE — only the *input* is
		// short-circuited.
		assert_eq!(
			eval_on("a.?.missing", &val("{ a: { b: 1 } }").await, &ctx).await.unwrap(),
			Value::None
		);
		// Only the top-level value is checked: an array that merely contains
		// NONE is present, so the tail maps over it.
		assert_eq!(
			eval_on("a.?.b", &val("{ a: [NONE, { b: 1 }] }").await, &ctx).await.unwrap(),
			val("[NONE, 1]").await
		);
		// A scalar is present too; the tail simply finds no field on it.
		assert_eq!(eval_on("a.?.b", &val("{ a: 42 }").await, &ctx).await.unwrap(), Value::None);
	}

	#[tokio::test]
	async fn nested_optionals_stop_at_the_first_none_or_null() {
		let ctx = root_ctx();
		// The planner nests one part per `.?`, so the inner chain is skipped as
		// soon as its own input is NONE/NULL.
		assert_eq!(
			eval_on("a.?.b.?.c", &val("{ a: { b: NULL } }").await, &ctx).await.unwrap(),
			Value::Null
		);
		assert_eq!(
			eval_on("a.?.b.?.c", &val("{ a: NULL }").await, &ctx).await.unwrap(),
			Value::Null
		);
		assert_eq!(
			eval_on("a.?.b.?.c", &val("{ a: { b: { c: 1 } } }").await, &ctx).await.unwrap(),
			Value::from(1)
		);
	}

	#[tokio::test]
	async fn an_empty_tail_passes_the_input_through() {
		// A trailing `.?` with nothing after it leaves the value alone, apart
		// from the same NONE/NULL short-circuit.
		let ctx = root_ctx();
		let part = OptionalChainPart {
			tail: vec![],
		};
		let base = EvalContext::from_exec_ctx(&ctx);
		for src in ["42", "'text'", "{ a: 1 }", "NULL", "NONE"] {
			let value = val(src).await;
			let out = part.evaluate(base.with_value(&value)).await.unwrap();
			assert_eq!(out, value, "an empty tail should pass {src} through");
		}
	}

	#[tokio::test]
	async fn declared_metadata_is_the_maximum_over_the_tail() {
		// The executor validates a plan's `required_context` before running it,
		// so an optional chain must report the strictest requirement of the
		// parts it hides — and nothing more when it hides none.
		let empty = OptionalChainPart {
			tail: vec![],
		};
		assert_eq!(empty.required_context(), ContextLevel::Root);
		assert_eq!(empty.access_mode(), AccessMode::ReadOnly);

		// A field access may dereference a record id, so it needs Database.
		let with_field = OptionalChainPart {
			tail: field_tail(&["a", "b"]),
		};
		assert_eq!(with_field.required_context(), ContextLevel::Database);
		assert_eq!(with_field.access_mode(), AccessMode::ReadOnly);
	}
}
