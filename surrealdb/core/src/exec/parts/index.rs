//! Computed index access part -- `[expr]`.

use std::sync::Arc;

use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, ContextLevel};
use crate::expr::FlowResult;
use crate::val::Value;

/// Computed index access - `[expr]`.
#[derive(Debug, Clone)]
pub struct IndexPart {
	pub expr: Arc<dyn PhysicalExpr>,
}
impl PhysicalExpr for IndexPart {
	fn name(&self) -> &'static str {
		"Index"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		self.expr.required_context()
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let value = ctx.current_value.unwrap_or(&Value::NONE);
			// Evaluate the index expression against the document root (if available)
			// so that dynamic key references like `[field]` or `[$param]` resolve
			// against the full document rather than the chain's current position.
			// This matches the old compute path where Part::Value evaluates the
			// expression with `doc` (the full cursor document).
			let index_ctx = if let Some(doc) = ctx.document_root {
				ctx.with_value(doc)
			} else {
				ctx
			};
			let index = self.expr.evaluate(index_ctx).await?;
			Ok(evaluate_index(value, &index)?)
		})
	}

	fn access_mode(&self) -> AccessMode {
		self.expr.access_mode()
	}
}

impl ToSql for IndexPart {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push('[');
		self.expr.fmt_sql(f, fmt);
		f.push(']');
	}
}

/// Index access on arrays, sets, objects, and record IDs.
pub(crate) fn evaluate_index(value: &Value, index: &Value) -> anyhow::Result<Value> {
	use crate::val::record_id::RecordIdKey;

	match (value, index) {
		// Array with numeric index
		(Value::Array(arr), Value::Number(n)) => {
			Ok(n.as_array_index().and_then(|idx| arr.get(idx).cloned()).unwrap_or(Value::None))
		}
		// Set with numeric index
		(Value::Set(set), Value::Number(n)) => {
			Ok(n.as_array_index().and_then(|idx| set.nth(idx).cloned()).unwrap_or(Value::None))
		}
		// Array with range
		(Value::Array(arr), Value::Range(range)) => {
			let slice = range
				.as_ref()
				.clone()
				.coerce_to_typed::<i64>()
				.map_err(|e| anyhow::anyhow!("Invalid range: {}", e))?
				.slice(arr.as_slice())
				.map(|s| Value::Array(s.to_vec().into()))
				.unwrap_or(Value::None);
			Ok(slice)
		}
		// Object with string key
		(Value::Object(obj), Value::String(key)) => {
			Ok(obj.get(key.as_str()).cloned().unwrap_or(Value::None))
		}
		// Object with numeric key (converted to string)
		(Value::Object(obj), Value::Number(n)) => {
			let key = n.to_string();
			Ok(obj.get(&key).cloned().unwrap_or(Value::None))
		}
		// RecordId with numeric index - only array keys support indexing
		(Value::RecordId(rid), Value::Number(n)) => match &rid.key {
			RecordIdKey::Array(arr) => {
				Ok(n.as_array_index().and_then(|idx| arr.get(idx).cloned()).unwrap_or(Value::None))
			}
			_ => Ok(Value::None),
		},
		_ => Ok(Value::None),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::{eval_on, physical_expr, root_ctx, val};
	use crate::val::Set;

	// =========================================================================
	// evaluate_index — container/index type matrix
	// =========================================================================

	#[tokio::test]
	async fn a_negative_or_fractional_numeric_index_is_none() {
		let arr = val("[1, 2, 3]").await;
		// Indexing is by position from the start only: a negative index does not
		// count back from the end, it misses.
		assert_eq!(evaluate_index(&arr, &Value::from(-1)).unwrap(), Value::None);
		// A float that is exactly an integer indexes; a fractional one misses.
		assert_eq!(evaluate_index(&arr, &Value::from(1.0)).unwrap(), Value::from(2));
		assert_eq!(evaluate_index(&arr, &Value::from(1.5)).unwrap(), Value::None);
	}

	#[tokio::test]
	async fn a_set_is_indexed_in_its_own_sorted_order() {
		// A set stores its members sorted and deduplicated, so position 0 is the
		// smallest member rather than the first one written.
		let set =
			Value::Set(Set::from_iter(vec![Value::from(30), Value::from(10), Value::from(20)]));
		assert_eq!(evaluate_index(&set, &Value::from(0)).unwrap(), Value::from(10));
		assert_eq!(evaluate_index(&set, &Value::from(2)).unwrap(), Value::from(30));
		assert_eq!(evaluate_index(&set, &Value::from(3)).unwrap(), Value::None);
	}

	#[tokio::test]
	async fn an_object_is_indexed_by_string_key_or_by_the_stringified_number() {
		let obj = val("{ a: 1, '2': 'two' }").await;
		assert_eq!(evaluate_index(&obj, &Value::from("a")).unwrap(), Value::from(1));
		assert_eq!(evaluate_index(&obj, &Value::from("nope")).unwrap(), Value::None);
		// A numeric index on an object is a key lookup by the number's own
		// rendering, not a positional read.
		assert_eq!(evaluate_index(&obj, &Value::from(2)).unwrap(), Value::from("two"));
		assert_eq!(evaluate_index(&obj, &Value::from(0)).unwrap(), Value::None);
	}

	#[tokio::test]
	async fn a_range_index_slices_an_array() {
		let arr = val("[1, 2, 3]").await;
		let range = val("0..2").await;
		assert!(matches!(range, Value::Range(_)), "expected a range, got {range:?}");
		assert_eq!(evaluate_index(&arr, &range).unwrap(), val("[1, 2]").await);
		assert_eq!(evaluate_index(&arr, &val("0..=1").await).unwrap(), val("[1, 2]").await);
		assert_eq!(evaluate_index(&arr, &val("1..").await).unwrap(), val("[2, 3]").await);
		assert_eq!(evaluate_index(&arr, &val("..").await).unwrap(), val("[1, 2, 3]").await);
		// An empty span yields an empty array, not NONE.
		assert_eq!(evaluate_index(&arr, &val("1..1").await).unwrap(), val("[]").await);
	}

	#[tokio::test]
	async fn a_range_reaching_past_the_end_yields_none_not_a_truncated_slice() {
		// The slice is all-or-nothing: an end bound beyond the last element
		// produces NONE rather than clamping to the array length.
		let arr = val("[1, 2, 3]").await;
		assert_eq!(evaluate_index(&arr, &val("0..10").await).unwrap(), Value::None);
		assert_eq!(evaluate_index(&arr, &val("5..").await).unwrap(), Value::None);
	}

	#[tokio::test]
	async fn a_range_whose_bounds_are_not_integers_is_an_error() {
		// Slicing needs integer bounds; anything else is reported rather than
		// silently treated as a miss.
		let arr = val("[1, 2, 3]").await;
		let err = evaluate_index(&arr, &val("'a'..'b'").await).unwrap_err();
		assert!(err.to_string().contains("Invalid range"), "got {err}");
	}

	#[tokio::test]
	async fn only_array_keyed_record_ids_support_numeric_indexing() {
		let array_keyed = val("doc:['a', 'b']").await;
		assert_eq!(evaluate_index(&array_keyed, &Value::from(1)).unwrap(), Value::from("b"));
		assert_eq!(evaluate_index(&array_keyed, &Value::from(9)).unwrap(), Value::None);
		// Other key kinds have no positional components.
		assert_eq!(evaluate_index(&val("doc:1").await, &Value::from(0)).unwrap(), Value::None);
		assert_eq!(evaluate_index(&val("doc:abc").await, &Value::from(0)).unwrap(), Value::None);
	}

	#[tokio::test]
	async fn an_index_of_an_unsupported_type_or_container_is_none() {
		let arr = val("[1, 2, 3]").await;
		let obj = val("{ a: 1 }").await;
		// Array indexed by a non-number, object indexed by a non-string/number.
		assert_eq!(evaluate_index(&arr, &Value::from("a")).unwrap(), Value::None);
		assert_eq!(evaluate_index(&obj, &Value::Bool(true)).unwrap(), Value::None);
		assert_eq!(evaluate_index(&arr, &Value::Bool(true)).unwrap(), Value::None);
		// Indexing something that is not a container at all.
		assert_eq!(evaluate_index(&Value::from(42), &Value::from(0)).unwrap(), Value::None);
		assert_eq!(evaluate_index(&Value::None, &Value::from(0)).unwrap(), Value::None);
		// A range only slices arrays.
		assert_eq!(evaluate_index(&obj, &val("0..1").await).unwrap(), Value::None);
	}

	// =========================================================================
	// IndexPart — the key expression is evaluated against the document root
	// =========================================================================

	#[tokio::test]
	async fn a_dynamic_key_resolves_against_the_document_root_not_the_chain_position() {
		let ctx = root_ctx();
		// Both the document root and the nested object the chain is standing on
		// have a `key` field. The key expression must read the document root's
		// value ('b'), so the result is 2 and never 1.
		let doc = val("{ key: 'b', nested: { key: 'a', a: 1, b: 2 } }").await;
		assert_eq!(eval_on("nested[key]", &doc, &ctx).await.unwrap(), Value::from(2));

		// Same for a key that only exists at the root.
		let doc = val("{ key: 'b', nested: { a: 1, b: 2 } }").await;
		assert_eq!(eval_on("nested[key]", &doc, &ctx).await.unwrap(), Value::from(2));
	}

	#[tokio::test]
	async fn a_param_key_is_read_from_the_execution_context() {
		let ctx = root_ctx().with_param("k", Value::from("b"));
		let doc = val("{ nested: { a: 1, b: 2 } }").await;
		assert_eq!(eval_on("nested[$k]", &doc, &ctx).await.unwrap(), Value::from(2));

		let ctx = root_ctx().with_param("i", Value::from(1));
		let doc = val("{ list: [10, 20, 30] }").await;
		assert_eq!(eval_on("list[$i]", &doc, &ctx).await.unwrap(), Value::from(20));

		// A parameter bound to a value that indexes nothing is a miss, not an
		// error.
		let ctx = root_ctx().with_param("k", Value::from("nope"));
		let doc = val("{ nested: { a: 1 } }").await;
		assert_eq!(eval_on("nested[$k]", &doc, &ctx).await.unwrap(), Value::None);
	}

	#[tokio::test]
	async fn a_computed_key_expression_is_evaluated_before_indexing() {
		let ctx = root_ctx();
		let doc = val("{ offset: 1, list: [10, 20, 30] }").await;
		assert_eq!(eval_on("list[offset + 1]", &doc, &ctx).await.unwrap(), Value::from(30));
	}

	#[tokio::test]
	async fn an_error_in_the_key_expression_propagates() {
		let ctx = root_ctx();
		let doc = val("{ list: [1, 2, 3] }").await;
		let err = eval_on("list[(THROW 'boom')]", &doc, &ctx).await.unwrap_err();
		assert!(err.to_string().contains("boom"), "got {err}");
	}

	#[tokio::test]
	async fn metadata_is_delegated_to_the_key_expression() {
		// The executor validates a plan's `required_context` before running it,
		// so an index over a literal must not drag the plan up to Database
		// level, while one whose key reads a field (and so may dereference a
		// record) must.
		let ctx = root_ctx();
		let literal_key = IndexPart {
			expr: physical_expr("1", &ctx).await,
		};
		assert_eq!(literal_key.required_context(), ContextLevel::Root);
		assert_eq!(literal_key.access_mode(), AccessMode::ReadOnly);

		let field_key = IndexPart {
			expr: physical_expr("k", &ctx).await,
		};
		assert_eq!(field_key.required_context(), ContextLevel::Database);
		assert_eq!(field_key.access_mode(), AccessMode::ReadOnly);
	}
}
