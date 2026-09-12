//! Optional chaining part -- `.?`.
//!
//! `OptionalChainPart` holds the rest of the path as its body and handles the
//! short-circuit itself: a `NONE` or `NULL` input is returned as it is and the
//! body never runs.

use std::sync::Arc;

use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, ContextLevel};
use crate::expr::FlowResult;
use crate::val::Value;

/// Optional chaining - `.?` followed by the rest of the path.
///
/// A `NONE` or `NULL` input is the result, keeping its own kind. Any other
/// input is the current value `then` is evaluated against.
///
/// For `a.?.b.?.c` the planner nests one part per `.?`: the body of the
/// first is the path `.b.?.c`, whose own optional part holds `.c`. A trailing
/// `.?` has the current value itself as its body.
#[derive(Debug, Clone)]
pub struct OptionalChainPart {
	/// The rest of the path, over the current value. Skipped entirely when
	/// the input is `NONE` or `NULL`.
	pub then: Arc<dyn PhysicalExpr>,
}
impl PhysicalExpr for OptionalChainPart {
	fn name(&self) -> &'static str {
		"OptionalChain"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		self.then.required_context()
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			// Short-circuit on None/Null, preserving the original kind
			if matches!(ctx.current_value, None | Some(Value::None | Value::Null)) {
				return Ok(ctx.current_value.cloned().unwrap_or(Value::None));
			}
			// The context already carries the input as its current value.
			self.then.evaluate(ctx).await
		})
	}

	fn access_mode(&self) -> AccessMode {
		self.then.access_mode()
	}
}

impl ToSql for OptionalChainPart {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str(".?");
		self.then.fmt_sql(f, fmt);
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::{eval_on, root_ctx, val};
	use crate::exec::parts::FieldPart;
	use crate::exec::physical_expr::{Current, Step};

	/// A body of plain field accesses over the current value, as the planner
	/// builds for `.?.a.b`.
	fn field_body(names: &[&str]) -> Arc<dyn PhysicalExpr> {
		let parts = names
			.iter()
			.map(|name| {
				Arc::new(FieldPart {
					name: (*name).to_owned(),
				}) as Arc<dyn PhysicalExpr>
			})
			.collect();
		Arc::new(Step::new(format!(".{}", names.join(".")), None, parts))
	}

	#[tokio::test]
	async fn none_and_null_skip_the_body_and_keep_their_own_kind() {
		let ctx = root_ctx();
		// NULL is the load-bearing case: the body would turn it into NONE, so a
		// NULL result is proof the body never ran.
		assert_eq!(eval_on("a.?.b", &val("{ a: NULL }").await, &ctx).await.unwrap(), Value::Null);
		assert_eq!(eval_on("a.?.b", &val("{ a: NONE }").await, &ctx).await.unwrap(), Value::None);
		// A missing field is NONE and short-circuits the same way.
		assert_eq!(eval_on("a.?.b", &val("{ }").await, &ctx).await.unwrap(), Value::None);
	}

	#[tokio::test]
	async fn a_present_value_is_threaded_through_the_whole_body() {
		let ctx = root_ctx();
		let doc = val("{ a: { b: { c: 7 } } }").await;
		assert_eq!(eval_on("a.?.b.c", &doc, &ctx).await.unwrap(), Value::from(7));
		// The body runs even when it produces NONE — only the *input* is
		// short-circuited.
		assert_eq!(
			eval_on("a.?.missing", &val("{ a: { b: 1 } }").await, &ctx).await.unwrap(),
			Value::None
		);
		// Only the top-level value is checked: an array that merely contains
		// NONE is present, so the body maps over it.
		assert_eq!(
			eval_on("a.?.b", &val("{ a: [NONE, { b: 1 }] }").await, &ctx).await.unwrap(),
			val("[NONE, 1]").await
		);
		// A scalar is present too; the body simply finds no field on it.
		assert_eq!(eval_on("a.?.b", &val("{ a: 42 }").await, &ctx).await.unwrap(), Value::None);
	}

	#[tokio::test]
	async fn nested_optionals_stop_at_the_first_none_or_null() {
		let ctx = root_ctx();
		// The planner nests one part per `.?`, so the inner body is skipped as
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
	async fn a_current_value_body_passes_the_input_through() {
		// A trailing `.?` with nothing after it leaves the value alone, apart
		// from the same NONE/NULL short-circuit.
		let ctx = root_ctx();
		let part = OptionalChainPart {
			then: Arc::new(Current),
		};
		let base = EvalContext::from_exec_ctx(&ctx);
		for src in ["42", "'text'", "{ a: 1 }", "NULL", "NONE"] {
			let value = val(src).await;
			let out = part.evaluate(base.with_value(&value)).await.unwrap();
			assert_eq!(out, value, "a current-value body should pass {src} through");
		}
	}

	#[tokio::test]
	async fn declared_metadata_is_that_of_the_body() {
		// The executor validates a plan's `required_context` before running it,
		// so an optional chain must report the strictest requirement of the
		// steps it hides — and nothing more when it hides none.
		let empty = OptionalChainPart {
			then: Arc::new(Current),
		};
		assert_eq!(empty.required_context(), ContextLevel::Root);
		assert_eq!(empty.access_mode(), AccessMode::ReadOnly);

		// A field access may dereference a record id, so it needs Database.
		let with_field = OptionalChainPart {
			then: field_body(&["a", "b"]),
		};
		assert_eq!(with_field.required_context(), ContextLevel::Database);
		assert_eq!(with_field.access_mode(), AccessMode::ReadOnly);
	}
}
