//! Where filtering part -- `[WHERE condition]`.

use std::sync::Arc;

use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, ContextLevel};
use crate::expr::FlowResult;
use crate::val::Value;

/// Filter predicate on arrays - `[WHERE condition]`.
#[derive(Debug, Clone)]
pub struct WherePart {
	pub predicate: Arc<dyn PhysicalExpr>,
	/// Whether the predicate references `$parent`. When false, we skip the
	/// per-element context allocation for binding `$parent`.
	pub needs_parent: bool,
}
impl PhysicalExpr for WherePart {
	fn name(&self) -> &'static str {
		"Where"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		self.predicate.required_context()
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			// Only bind $parent when the predicate actually references it,
			// avoiding a Value::clone() + context allocation per element for
			// the common case (e.g. `[WHERE age > 30]`).
			let parent_ctx = if self.needs_parent
				&& let Some(parent) = ctx.document_root
			{
				Some(ctx.exec_ctx.with_param("parent", parent.clone()))
			} else {
				None
			};
			let ctx = if let Some(ref pc) = parent_ctx {
				EvalContext {
					exec_ctx: pc,
					current_value: ctx.current_value,
					local_params: ctx.local_params,
					recursion_ctx: ctx.recursion_ctx,
					document_root: ctx.document_root,
					skip_fetch_perms: ctx.skip_fetch_perms,
					computing_record: ctx.computing_record,
					plan_depth: ctx.plan_depth,
				}
			} else {
				ctx
			};

			let value = ctx.current_value.cloned().unwrap_or(Value::None);
			match value {
				Value::Array(arr) => {
					let mut result = Vec::new();
					for item in arr.iter() {
						let item_ctx = ctx.with_value(item);
						let matches = self.predicate.evaluate(item_ctx).await?.is_truthy();
						if matches {
							result.push(item.clone());
						}
					}
					Ok(Value::Array(result.into()))
				}
				// A filtered set stays a set, so `<set>[..][WHERE ..]` keeps its
				// deduplicating type instead of decaying to an array.
				Value::Set(set) => {
					let mut result = crate::val::Set::new();
					for item in set.iter() {
						let item_ctx = ctx.with_value(item);
						let matches = self.predicate.evaluate(item_ctx).await?.is_truthy();
						if matches {
							result.insert(item.clone());
						}
					}
					Ok(Value::Set(result))
				}
				// `[WHERE ..]` selects from a collection. Applied to anything
				// else -- a scalar, an object, a record id, NONE -- there is
				// nothing to select from, so the path yields NONE rather than
				// promoting the value to a one-element collection.
				_ => Ok(Value::None),
			}
		})
	}

	fn access_mode(&self) -> AccessMode {
		self.predicate.access_mode()
	}
}

impl ToSql for WherePart {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("[WHERE ");
		self.predicate.fmt_sql(f, fmt);
		f.push(']');
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ExecutionContext;
	use crate::exec::operators::test_util::{eval_on, physical_expr, root_ctx, val};
	use crate::exec::physical_expr::IdiomExpr;
	use crate::expr::ControlFlow;
	use crate::val::Set;

	/// Evaluate a hand-built `WherePart` against `value`, with `root` as the
	/// document root the enclosing idiom started from.
	async fn filter_with_root(
		predicate: &str,
		needs_parent: bool,
		value: &Value,
		root: &Value,
		ctx: &ExecutionContext,
	) -> Value {
		let part = WherePart {
			predicate: physical_expr(predicate, ctx).await,
			needs_parent,
		};
		let base = EvalContext::from_exec_ctx(ctx);
		let eval_ctx = EvalContext {
			document_root: Some(root),
			..base.with_value(value)
		};
		part.evaluate(eval_ctx).await.expect("filter should succeed")
	}

	/// As [`filter_with_root`], with `value` doubling as the document root.
	async fn filter(
		predicate: &str,
		needs_parent: bool,
		value: &Value,
		ctx: &ExecutionContext,
	) -> Value {
		filter_with_root(predicate, needs_parent, value, value, ctx).await
	}

	// =========================================================================
	// Filtering an array
	// =========================================================================

	#[tokio::test]
	async fn an_array_of_objects_keeps_only_the_matching_elements_in_order() {
		let ctx = root_ctx();
		let doc = val("{ people: [{ age: 25 }, { age: 35 }, { age: 45 }] }").await;
		assert_eq!(
			eval_on("people[WHERE age > 30]", &doc, &ctx).await.unwrap(),
			val("[{ age: 35 }, { age: 45 }]").await
		);
		// No match yields an empty array, not NONE.
		assert_eq!(eval_on("people[WHERE age > 100]", &doc, &ctx).await.unwrap(), val("[]").await);
		// An empty input yields an empty array.
		let empty = val("{ people: [] }").await;
		assert_eq!(eval_on("people[WHERE age > 30]", &empty, &ctx).await.unwrap(), val("[]").await);
	}

	#[tokio::test]
	async fn an_element_whose_predicate_field_is_absent_is_dropped_rather_than_erroring() {
		let ctx = root_ctx();
		let doc = val("{ people: [{ age: 35 }, { name: 'x' }] }").await;
		assert_eq!(
			eval_on("people[WHERE age > 30]", &doc, &ctx).await.unwrap(),
			val("[{ age: 35 }]").await
		);
	}

	#[tokio::test]
	async fn an_element_is_kept_on_any_truthy_predicate_value_not_only_true() {
		let ctx = root_ctx();
		// The predicate result goes through the same truthiness rules as
		// anywhere else: zero, the empty string, the empty array, NONE and NULL
		// all drop the element.
		let doc = val("{ list: [0, 1, '', 'x', NONE, NULL, [], [1]] }").await;
		assert_eq!(
			eval_on("list[WHERE $this]", &doc, &ctx).await.unwrap(),
			val("[1, 'x', [1]]").await
		);
	}

	#[tokio::test]
	async fn this_is_bound_to_the_element_under_test() {
		let ctx = root_ctx();
		let doc = val("{ list: [1, 2, 3, 4] }").await;
		assert_eq!(
			eval_on("list[WHERE $this > 2]", &doc, &ctx).await.unwrap(),
			val("[3, 4]").await
		);
	}

	// =========================================================================
	// $parent
	// =========================================================================

	#[tokio::test]
	async fn parent_resolves_to_the_enclosing_document() {
		let ctx = root_ctx();
		let doc = val("{ min: 30, people: [{ age: 25 }, { age: 35 }] }").await;
		assert_eq!(
			eval_on("people[WHERE age > $parent.min]", &doc, &ctx).await.unwrap(),
			val("[{ age: 35 }]").await
		);
	}

	#[tokio::test]
	async fn the_planner_marks_only_predicates_that_reference_parent() {
		let ctx = root_ctx();
		for (src, expected) in
			[("people[WHERE age > $parent.min]", true), ("people[WHERE age > 30]", false)]
		{
			let expr = physical_expr(src, &ctx).await;
			let idiom =
				expr.downcast_ref::<IdiomExpr>().expect("an idiom compiles to an IdiomExpr");
			let part = idiom
				.parts
				.last()
				.expect("the idiom has parts")
				.downcast_ref::<WherePart>()
				.expect("the trailing part is the filter");
			assert_eq!(part.needs_parent, expected, "needs_parent for {src}");
		}
	}

	#[tokio::test]
	async fn skipping_the_parent_binding_cannot_change_what_the_predicate_sees() {
		// `needs_parent` only decides whether `$parent` is bound as a parameter
		// per element. Resolution also falls back to the document root, so the
		// two settings must agree on the same input.
		let ctx = root_ctx();
		let root = val("{ min: 30 }").await;
		let people = val("[{ age: 25 }, { age: 35 }]").await;

		let bound = filter_with_root("age > $parent.min", true, &people, &root, &ctx).await;
		let unbound = filter_with_root("age > $parent.min", false, &people, &root, &ctx).await;
		assert_eq!(bound, val("[{ age: 35 }]").await);
		assert_eq!(unbound, bound);
	}

	// =========================================================================
	// Inputs that are not arrays
	// =========================================================================

	#[tokio::test]
	async fn a_filter_on_a_non_collection_yields_none() {
		// There is nothing to select from, so a matching predicate does not
		// promote the value to a one-element collection.
		let ctx = root_ctx();
		assert_eq!(
			eval_on("a[WHERE $this > 2]", &val("{ a: 5 }").await, &ctx).await.unwrap(),
			Value::None
		);
		assert_eq!(
			eval_on("a[WHERE $this > 2]", &val("{ a: 1 }").await, &ctx).await.unwrap(),
			Value::None
		);
		// An object is not a collection of its entries either.
		assert_eq!(
			eval_on("a[WHERE age > 30]", &val("{ a: { age: 35 } }").await, &ctx).await.unwrap(),
			Value::None
		);
	}

	#[tokio::test]
	async fn a_filter_on_a_missing_field_yields_none() {
		// A missing field is NONE, which is not a collection, so the predicate
		// does not decide the result.
		let ctx = root_ctx();
		assert_eq!(
			eval_on("a[WHERE $this = NONE]", &val("{ }").await, &ctx).await.unwrap(),
			Value::None
		);
		assert_eq!(
			eval_on("a[WHERE $this != NONE]", &val("{ }").await, &ctx).await.unwrap(),
			Value::None
		);
	}

	#[tokio::test]
	async fn a_filter_on_a_set_keeps_the_matching_members_as_a_set() {
		// Sets are filtered member-wise like arrays, and the deduplicating type
		// survives the filter.
		let ctx = root_ctx();
		let set = Value::Set(Set::from_iter(vec![Value::from(1), Value::from(2), Value::from(3)]));
		assert_eq!(
			filter("$this > 1", false, &set, &ctx).await,
			Value::Set(Set::from_iter(vec![Value::from(2), Value::from(3)]))
		);
		assert_eq!(
			filter("$this > 10", false, &set, &ctx).await,
			Value::Set(Set::from_iter(vec![]))
		);
	}

	// =========================================================================
	// Error and control-flow propagation
	// =========================================================================

	#[tokio::test]
	async fn an_error_raised_by_the_predicate_aborts_the_filter() {
		let ctx = root_ctx();
		let doc = val("{ list: [1, 2, 3] }").await;
		let err = eval_on("list[WHERE THROW 'boom']", &doc, &ctx).await.unwrap_err();
		assert!(err.to_string().contains("boom"), "got {err}");
	}

	#[tokio::test]
	async fn a_control_flow_signal_from_the_predicate_propagates_out_of_the_filter() {
		// The predicate's control flow is not caught here: the signal travels up
		// to whatever construct owns it, rather than being read as a truthiness
		// value.
		let ctx = root_ctx();
		let doc = val("{ list: [1, 2, 3] }").await;
		let err = eval_on("list[WHERE BREAK]", &doc, &ctx).await.unwrap_err();
		assert!(matches!(err, ControlFlow::Break), "got {err:?}");

		// A RETURN is not absorbed as the predicate's value either; it leaves
		// the filter carrying its own value. The legacy `compute` path catches a
		// RETURN at this site and uses the returned value as the predicate
		// result instead.
		let err = eval_on("list[WHERE RETURN true]", &doc, &ctx).await.unwrap_err();
		assert!(matches!(&err, ControlFlow::Return(v) if v == &Value::Bool(true)), "got {err:?}");
	}
}
