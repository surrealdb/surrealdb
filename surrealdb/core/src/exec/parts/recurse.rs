//! Recursion parts -- `{min..max}`, `{..}`, and `@` (RepeatRecurse).

use std::sync::Arc;

use futures::StreamExt;
use surrealdb_types::{SqlFormat, ToSql};

// Re-export evaluation functions from the operator module so that
// RepeatRecursePart (and any other callers) can still reach them.
pub(crate) use crate::exec::operators::recursion::evaluate_repeat_recurse;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, BoxFut, ContextLevel, ExecOperator};
use crate::expr::FlowResult;
use crate::val::Value;

// ============================================================================
// PhysicalRecurseInstruction -- shared enum
// ============================================================================

/// Instruction for how to handle recursion results.
#[derive(Debug, Clone)]
pub enum PhysicalRecurseInstruction {
	/// Default: return the final values after recursion
	Default,

	/// Collect all unique nodes encountered during traversal
	Collect,

	/// Return all paths as arrays of arrays
	Path,

	/// Find shortest path to a target node
	Shortest {
		/// Expression that evaluates to the target RecordId
		target: Arc<dyn PhysicalExpr>,
	},
}

// ============================================================================
// RecursePart
// ============================================================================

/// Recursive graph traversal - `{min..max}`.
///
/// Wraps a `RecursionOp` ExecOperator that holds the body operator chain
/// and implements the recursion iteration loop. The operator is exposed
/// via `embedded_operators()` for EXPLAIN display.
#[derive(Debug, Clone)]
pub struct RecursePart {
	/// The pre-planned recursion operator tree.
	pub op: Arc<dyn ExecOperator>,
}
impl PhysicalExpr for RecursePart {
	fn name(&self) -> &'static str {
		"Recurse"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		self.op.required_context()
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let value = ctx.current_value.cloned().unwrap_or(Value::None);

			// Create a new execution context with the current value set.
			// The RecursionOp reads this to seed the recursion.
			let bound_ctx = ctx.exec_ctx.with_current_value(value);
			let bound_ctx = if ctx.skip_fetch_perms {
				bound_ctx.with_skip_fetch_perms(true)
			} else {
				bound_ctx
			};

			// Execute the recursion operator
			let mut stream = self.op.execute(&bound_ctx).map_err(|e| match e {
				crate::expr::ControlFlow::Err(e) => crate::expr::ControlFlow::Err(e),
				other => other,
			})?;

			// Collect results from the stream
			let mut result = Value::None;
			while let Some(batch_result) = stream.next().await {
				let batch = batch_result?;
				// RecursionOp yields a single batch with the recursion result
				if let Some(v) = batch.into_iter().next() {
					result = v;
				}
			}

			Ok(result)
		})
	}

	fn access_mode(&self) -> AccessMode {
		self.op.access_mode()
	}

	fn embedded_operators(&self) -> Vec<(&str, &Arc<dyn ExecOperator>)> {
		vec![("recurse", &self.op)]
	}
}

impl ToSql for RecursePart {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		// Delegate to the operator's attrs for SQL formatting
		let attrs = self.op.attrs();
		f.push_str(".{");

		// Extract depth from attrs
		for (k, v) in &attrs {
			if k == "depth" {
				// Parse depth to build SQL
				if v.contains("..") {
					let parts: Vec<&str> = v.split("..").collect();
					if parts.len() == 2 {
						let min = parts[0];
						let max = parts[1];
						if min != "1" {
							f.push_str(min);
						}
						f.push_str("..");
						f.push_str(max);
					}
				} else {
					// Single value: exact depth (e.g., "3" → ".{3}" meaning min=3, max=3).
					// Do NOT prefix with ".." -- that would produce ".{..3}" meaning min=1, max=3.
					f.push_str(v);
				}
			}
		}

		// Extract instruction
		for (k, v) in &attrs {
			if k == "instruction" {
				match v.as_str() {
					"default" => {}
					"collect" => f.push_str("+collect"),
					"path" => f.push_str("+path"),
					"shortest" => f.push_str("+shortest=..."),
					_ => {}
				}
			}
		}

		f.push('}');
	}
}

// ============================================================================
// RepeatRecursePart -- `@`
// ============================================================================

/// RepeatRecurse marker - `@`.
///
/// When encountered during path evaluation inside a recursion context,
/// this part re-invokes the recursion evaluator on the current value
/// with incremented depth.
#[derive(Debug, Clone)]
pub struct RepeatRecursePart;
impl PhysicalExpr for RepeatRecursePart {
	fn name(&self) -> &'static str {
		"RepeatRecurse"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let value = ctx.current_value.unwrap_or(&Value::NONE);
			evaluate_repeat_recurse(value, ctx).await
		})
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}
}

impl ToSql for RepeatRecursePart {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push('@');
	}
}

// ============================================================================
// Helpers
// ============================================================================

/// Helper function to create a hash for value deduplication in graph traversal.
///
/// Optimized for the common case of RecordId values, which are the primary
/// target for cycle detection in graph traversal.
pub(crate) fn value_hash(value: &Value) -> u64 {
	use std::hash::{Hash, Hasher};
	let mut hasher = std::collections::hash_map::DefaultHasher::new();

	match value {
		Value::RecordId(rid) => {
			0u8.hash(&mut hasher);
			rid.hash(&mut hasher);
		}
		Value::None => {
			1u8.hash(&mut hasher);
		}
		Value::Null => {
			2u8.hash(&mut hasher);
		}
		Value::Bool(b) => {
			3u8.hash(&mut hasher);
			b.hash(&mut hasher);
		}
		Value::String(s) => {
			4u8.hash(&mut hasher);
			s.hash(&mut hasher);
		}
		Value::Number(n) => {
			5u8.hash(&mut hasher);
			n.to_string().hash(&mut hasher);
		}
		Value::Uuid(u) => {
			6u8.hash(&mut hasher);
			u.0.hash(&mut hasher);
		}
		Value::Array(arr) => {
			7u8.hash(&mut hasher);
			arr.len().hash(&mut hasher);
			for (i, v) in arr.iter().enumerate() {
				if i >= 8 {
					break;
				}
				value_hash(v).hash(&mut hasher);
			}
		}
		Value::Object(obj) => {
			8u8.hash(&mut hasher);
			obj.len().hash(&mut hasher);
			for (i, (k, v)) in obj.iter().enumerate() {
				if i >= 8 {
					break;
				}
				k.hash(&mut hasher);
				value_hash(v).hash(&mut hasher);
			}
		}
		_ => {
			255u8.hash(&mut hasher);
			format!("{:?}", value).hash(&mut hasher);
		}
	}

	hasher.finish()
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use rust_decimal::Decimal;

	use super::*;
	use crate::exec::operators::recursion::RecursionOp;
	use crate::exec::operators::test_util::{TestDb, ValuesOperator, eval, eval_on, root_ctx, val};
	use crate::exec::physical_expr::RecursionCtx;
	use crate::val::{Number, Object, RecordId, RecordIdKey};

	fn rid(table: &str, key: i64) -> Value {
		Value::RecordId(RecordId {
			table: table.into(),
			key: RecordIdKey::Number(key),
		})
	}

	fn arr(values: Vec<Value>) -> Value {
		Value::from(values)
	}

	// =========================================================================
	// value_hash -- the identity used for per-level dedup and the assembly cache
	// =========================================================================

	#[test]
	fn value_hash_separates_the_three_falsey_variants() {
		// Cycle detection keys on this hash, so NONE, NULL and `false` must not
		// be conflated: each variant carries its own discriminant byte.
		let none = value_hash(&Value::None);
		let null = value_hash(&Value::Null);
		let no = value_hash(&Value::Bool(false));
		assert_ne!(none, null);
		assert_ne!(none, no);
		assert_ne!(null, no);
	}

	#[test]
	fn value_hash_treats_a_record_id_key_type_as_part_of_its_identity() {
		// RecordId hashes through its own `Hash` impl, so the table and the key
		// (including the key's type) all take part.
		let numeric = rid("person", 1);
		let string_key = Value::RecordId(RecordId {
			table: "person".into(),
			key: RecordIdKey::String("1".into()),
		});
		let other_table = rid("company", 1);

		assert_eq!(value_hash(&numeric), value_hash(&rid("person", 1)));
		assert_ne!(value_hash(&numeric), value_hash(&string_key));
		assert_ne!(value_hash(&numeric), value_hash(&other_table));
	}

	#[test]
	fn value_hash_of_a_number_is_its_decimal_rendering_not_its_representation() {
		// Numbers are hashed via `to_string()`, so an integer, a float and a
		// decimal that all render as "1" share one hash. This is what makes
		// `1` and `1f` a single node during traversal.
		let int = Value::Number(Number::Int(1));
		let float = Value::Number(Number::Float(1.0));
		let decimal = Value::Number(Number::Decimal(Decimal::from(1)));

		assert_eq!(value_hash(&int), value_hash(&float));
		assert_eq!(value_hash(&int), value_hash(&decimal));
	}

	#[test]
	fn value_hash_splits_numerically_equal_numbers_that_render_differently() {
		// The rendering is not canonicalised, so trailing decimal scale and a
		// negative zero produce different hashes from the numerically equal
		// plain integer. Two values that compare equal therefore hash apart,
		// and the traversal treats them as distinct nodes.
		let one = Value::Number(Number::Int(1));
		let one_scaled = Value::Number(Number::Decimal("1.0".parse().expect("decimal literal")));
		assert_eq!(one, one_scaled, "these compare equal");
		assert_ne!(value_hash(&one), value_hash(&one_scaled), "but they do not hash equal");

		let zero = Value::Number(Number::Int(0));
		let negative_zero = Value::Number(Number::Float(-0.0));
		assert_eq!(zero, negative_zero, "these compare equal");
		assert_ne!(value_hash(&zero), value_hash(&negative_zero), "but they do not hash equal");
	}

	#[test]
	fn value_hash_of_an_array_covers_its_length_and_only_the_first_eight_items() {
		let short = arr(vec![Value::from(1), Value::from(2)]);
		let longer = arr(vec![Value::from(1), Value::from(2), Value::from(3)]);
		assert_ne!(value_hash(&short), value_hash(&longer), "length takes part in the hash");

		// Element hashing stops after index 7. Two nine-element arrays that
		// share their first eight elements collide, so a traversal that
		// deduplicated on such values would treat them as the same node.
		let mut a: Vec<Value> = (0..9).map(Value::from).collect();
		let mut b = a.clone();
		a[8] = Value::from(100);
		b[8] = Value::from(200);
		assert_eq!(value_hash(&arr(a)), value_hash(&arr(b)));
	}

	#[test]
	fn value_hash_of_an_object_covers_its_size_and_only_the_first_eight_entries() {
		let small = Value::Object(Object::from_iter([("a".to_string(), Value::from(1))]));
		let bigger = Value::Object(Object::from_iter([
			("a".to_string(), Value::from(1)),
			("b".to_string(), Value::from(2)),
		]));
		assert_ne!(value_hash(&small), value_hash(&bigger), "entry count takes part in the hash");

		// Entries are visited in key order and hashing stops after the eighth,
		// so two nine-entry objects differing only in the last key collide.
		let entries = |last: Value| {
			let mut e: Vec<(String, Value)> =
				(0..8).map(|i| (format!("k{i}"), Value::from(i))).collect();
			e.push(("k8".to_string(), last));
			Value::Object(Object::from_iter(e))
		};
		assert_eq!(value_hash(&entries(Value::from(100))), value_hash(&entries(Value::from(200))));
	}

	#[tokio::test]
	async fn value_hash_falls_back_to_the_debug_rendering_for_untagged_variants() {
		// Variants with no dedicated arm share discriminant 255 and are told
		// apart by their `Debug` output alone.
		let one_second = val("1s").await;
		let two_seconds = val("2s").await;
		assert!(matches!(one_second, Value::Duration(_)), "fixture must hit the fallback arm");
		assert_ne!(value_hash(&one_second), value_hash(&two_seconds));

		// A tagged variant with a similar rendering stays distinct.
		let text = Value::String("1s".into());
		assert_ne!(value_hash(&one_second), value_hash(&text));
	}

	// =========================================================================
	// RepeatRecursePart -- the two-phase discovery / assembly protocol
	// =========================================================================

	fn discovery(
		min_depth: u32,
		depth: u32,
		sink: &Arc<parking_lot::Mutex<Vec<Value>>>,
	) -> RecursionCtx {
		RecursionCtx {
			min_depth,
			depth,
			discovery_sink: Some(Arc::clone(sink)),
			assembly_cache: None,
		}
	}

	fn assembly(min_depth: u32, depth: u32, cache: HashMap<u64, Value>) -> RecursionCtx {
		RecursionCtx {
			min_depth,
			depth,
			discovery_sink: None,
			assembly_cache: Some(Arc::new(cache)),
		}
	}

	async fn repeat_recurse(value: &Value, rec: Option<RecursionCtx>) -> FlowResult<Value> {
		let ctx = root_ctx();
		let eval_ctx = EvalContext::from_exec_ctx(&ctx).with_value(value);
		let eval_ctx = match rec {
			Some(rec) => eval_ctx.with_recursion_ctx(rec),
			None => eval_ctx,
		};
		RepeatRecursePart.evaluate(eval_ctx).await
	}

	#[tokio::test]
	async fn repeat_recurse_outside_a_recursion_context_is_rejected() {
		let err = repeat_recurse(&rid("person", 1), None).await.unwrap_err();
		assert!(
			err.to_string().contains("repeat recurse symbol"),
			"expected the unsupported-`@` error, got {err}"
		);
	}

	#[tokio::test]
	async fn repeat_recurse_with_neither_phase_selected_is_rejected() {
		// `RecursionOp` always sets exactly one of the two fields; a context
		// with neither is not a reachable state and must not silently no-op.
		let neither = RecursionCtx {
			min_depth: 1,
			depth: 0,
			discovery_sink: None,
			assembly_cache: None,
		};
		let err = repeat_recurse(&rid("person", 1), Some(neither)).await.unwrap_err();
		assert!(
			err.to_string().contains("repeat recurse symbol"),
			"expected the unsupported-`@` error, got {err}"
		);
	}

	#[tokio::test]
	async fn discovery_writes_record_targets_to_the_sink_and_returns_its_input_unchanged() {
		let sink = Arc::new(parking_lot::Mutex::new(Vec::new()));
		let input = arr(vec![rid("person", 1), rid("person", 2)]);

		let out = repeat_recurse(&input, Some(discovery(1, 0, &sink))).await.unwrap();

		// The discovery phase reads the sink, not the return value, so `@`
		// hands its input straight back.
		assert_eq!(out, input);
		assert_eq!(*sink.lock(), vec![rid("person", 1), rid("person", 2)]);
	}

	#[tokio::test]
	async fn discovery_skips_dead_ends_without_touching_the_sink() {
		let sink = Arc::new(parking_lot::Mutex::new(Vec::new()));

		// A bare dead end contributes nothing.
		assert_eq!(
			repeat_recurse(&Value::None, Some(discovery(1, 0, &sink))).await.unwrap(),
			Value::None
		);
		assert!(sink.lock().is_empty());

		// Dead ends inside an array are filtered out element-wise.
		let mixed = arr(vec![Value::None, rid("person", 1), Value::Null]);
		repeat_recurse(&mixed, Some(discovery(1, 0, &sink))).await.unwrap();
		assert_eq!(*sink.lock(), vec![rid("person", 1)]);
	}

	#[tokio::test]
	async fn discovery_rejects_a_non_record_recursion_target() {
		// Recursion is only defined over record ids; anything else that is not
		// a dead end is a hard error rather than a silent stop.
		let sink = Arc::new(parking_lot::Mutex::new(Vec::new()));
		let err = repeat_recurse(&Value::from(7), Some(discovery(1, 0, &sink))).await.unwrap_err();
		assert!(
			err.to_string().contains("Expected a record ID"),
			"expected InvalidRecursionTarget, got {err}"
		);
		assert!(sink.lock().is_empty());

		// The same rule applies per element inside an array, and it aborts the
		// whole call rather than writing the valid prefix.
		let err = repeat_recurse(
			&arr(vec![rid("person", 1), Value::from("nope")]),
			Some(discovery(1, 0, &sink)),
		)
		.await
		.unwrap_err();
		assert!(
			err.to_string().contains("Expected a record ID"),
			"expected InvalidRecursionTarget, got {err}"
		);
		assert!(sink.lock().is_empty());
	}

	#[tokio::test]
	async fn assembly_substitutes_the_cached_result_for_each_discovered_element() {
		let mut cache = HashMap::new();
		cache.insert(value_hash(&rid("person", 1)), Value::from("one"));
		cache.insert(value_hash(&rid("person", 2)), Value::from("two"));

		let out = repeat_recurse(
			&arr(vec![rid("person", 1), rid("person", 2)]),
			Some(assembly(1, 0, cache.clone())),
		)
		.await
		.unwrap();
		assert_eq!(out, arr(vec![Value::from("one"), Value::from("two")]));

		// A scalar input resolves to the single cached entry.
		let out = repeat_recurse(&rid("person", 2), Some(assembly(1, 0, cache))).await.unwrap();
		assert_eq!(out, Value::from("two"));
	}

	#[tokio::test]
	async fn assembly_flattens_one_level_and_drops_dead_ends_from_the_assembled_array() {
		// The assembled array goes through `clean_iteration`: final entries are
		// removed and one level of nesting is flattened, so a level's results
		// arrive as a flat list.
		let mut cache = HashMap::new();
		cache.insert(value_hash(&rid("person", 1)), arr(vec![Value::from(1), Value::from(2)]));
		cache.insert(value_hash(&rid("person", 2)), Value::None);
		cache.insert(value_hash(&rid("person", 3)), arr(vec![Value::from(3)]));

		let out = repeat_recurse(
			&arr(vec![rid("person", 1), rid("person", 2), rid("person", 3)]),
			Some(assembly(1, 0, cache)),
		)
		.await
		.unwrap();
		assert_eq!(out, arr(vec![Value::from(1), Value::from(2), Value::from(3)]));
	}

	#[tokio::test]
	async fn assembly_skips_an_element_that_was_never_discovered() {
		// Discovery and assembly walk the same levels, so a miss should not
		// happen; when it does the element is dropped rather than turned into
		// NONE, keeping the assembled array free of holes.
		let mut cache = HashMap::new();
		cache.insert(value_hash(&rid("person", 1)), Value::from("one"));

		let out = repeat_recurse(
			&arr(vec![rid("person", 1), rid("person", 99)]),
			Some(assembly(1, 0, cache)),
		)
		.await
		.unwrap();
		assert_eq!(out, arr(vec![Value::from("one")]));
	}

	#[tokio::test]
	async fn assembly_of_a_scalar_miss_yields_none_rather_than_an_error() {
		let out =
			repeat_recurse(&rid("person", 1), Some(assembly(1, 0, HashMap::new()))).await.unwrap();
		assert_eq!(out, Value::None);
	}

	#[tokio::test]
	async fn assembly_eliminates_a_dead_end_subtree_below_min_depth() {
		// `@` compares the depth of the call it would have made (`depth + 1`)
		// against `min_depth`. A dead end below that threshold raises the path
		// elimination signal, which the assembly loop turns into NONE so the
		// parent level's `clean_iteration` can drop the sub-tree.
		let mut cache = HashMap::new();
		cache.insert(value_hash(&rid("person", 1)), Value::None);

		let err = repeat_recurse(&rid("person", 1), Some(assembly(5, 0, cache.clone())))
			.await
			.unwrap_err();
		assert!(
			err.to_string().contains("path elimination"),
			"expected the elimination signal, got {err}"
		);

		// An array whose every entry resolves to a dead end cleans down to `[]`,
		// which is itself final, so it eliminates too.
		let err = repeat_recurse(&arr(vec![rid("person", 1)]), Some(assembly(5, 0, cache.clone())))
			.await
			.unwrap_err();
		assert!(
			err.to_string().contains("path elimination"),
			"expected the elimination signal, got {err}"
		);

		// A dead end handed straight to `@` eliminates as well.
		let err = repeat_recurse(&Value::None, Some(assembly(5, 0, cache))).await.unwrap_err();
		assert!(
			err.to_string().contains("path elimination"),
			"expected the elimination signal, got {err}"
		);
	}

	#[tokio::test]
	async fn assembly_keeps_a_dead_end_once_min_depth_is_satisfied() {
		let mut cache = HashMap::new();
		cache.insert(value_hash(&rid("person", 1)), Value::None);

		// depth 0 -> the recursive call would be depth 1, which is not below
		// min_depth 1, so the dead end is a legitimate leaf.
		let out =
			repeat_recurse(&rid("person", 1), Some(assembly(1, 0, cache.clone()))).await.unwrap();
		assert_eq!(out, Value::None);

		let out = repeat_recurse(&Value::None, Some(assembly(1, 0, cache))).await.unwrap();
		assert_eq!(out, Value::None);
	}

	// =========================================================================
	// RecursePart -- draining the recursion operator
	// =========================================================================

	#[tokio::test]
	async fn recurse_part_yields_none_when_its_operator_emits_no_value() {
		// `RecursionOp` contracts to emit exactly one batch holding one value.
		// An empty stream must surface as NONE, not as an error or a panic.
		let ctx = root_ctx();
		let part = RecursePart {
			op: ValuesOperator::new(vec![]),
		};
		let out = part.evaluate(EvalContext::from_exec_ctx(&ctx)).await.unwrap();
		assert_eq!(out, Value::None);
	}

	#[tokio::test]
	async fn recurse_part_takes_only_the_first_value_of_each_batch() {
		// The drain loop reads one value per batch and overwrites `result` with
		// it, so anything after the first value in a batch is discarded. That is
		// sound only because `RecursionOp` emits a single-value batch.
		let ctx = root_ctx();
		let part = RecursePart {
			op: ValuesOperator::new(vec![Value::from(1), Value::from(2)]),
		};
		let out = part.evaluate(EvalContext::from_exec_ctx(&ctx)).await.unwrap();
		assert_eq!(out, Value::from(1));
	}

	// =========================================================================
	// RecursePart -- SQL rendering of the depth range
	// =========================================================================

	fn recurse_sql(min: u32, max: Option<u32>, instruction: PhysicalRecurseInstruction) -> String {
		let op: Arc<dyn ExecOperator> =
			Arc::new(RecursionOp::new(None, vec![], min, max, instruction, false, false));
		RecursePart {
			op,
		}
		.to_sql()
	}

	#[test]
	fn an_exact_depth_renders_without_a_range_prefix() {
		// `.{3}` means min=3, max=3. Rendering it as `.{..3}` would silently
		// widen the range to 1..3.
		assert_eq!(recurse_sql(3, Some(3), PhysicalRecurseInstruction::Default), ".{3}");
		assert_eq!(recurse_sql(1, Some(1), PhysicalRecurseInstruction::Default), ".{1}");
	}

	#[test]
	fn a_range_omits_a_minimum_of_one_and_an_unbounded_maximum() {
		assert_eq!(recurse_sql(1, Some(5), PhysicalRecurseInstruction::Default), ".{..5}");
		assert_eq!(recurse_sql(1, None, PhysicalRecurseInstruction::Default), ".{..}");
		assert_eq!(recurse_sql(2, Some(5), PhysicalRecurseInstruction::Default), ".{2..5}");
		assert_eq!(recurse_sql(2, None, PhysicalRecurseInstruction::Default), ".{2..}");
	}

	#[tokio::test]
	async fn the_instruction_is_appended_after_the_depth_range() {
		assert_eq!(recurse_sql(1, Some(3), PhysicalRecurseInstruction::Collect), ".{..3+collect}");
		assert_eq!(recurse_sql(1, Some(3), PhysicalRecurseInstruction::Path), ".{..3+path}");

		let ctx = root_ctx();
		let target = crate::exec::operators::test_util::physical_expr("person:tobie", &ctx).await;
		assert_eq!(
			recurse_sql(
				1,
				Some(3),
				PhysicalRecurseInstruction::Shortest {
					target
				}
			),
			".{..3+shortest=...}"
		);
	}

	// =========================================================================
	// End-to-end: the two phases over real record links
	// =========================================================================

	/// A three-deep chain of record links: `node:a -> node:b -> node:c`.
	async fn chain_db() -> TestDb {
		TestDb::new(
			"DEFINE TABLE node SCHEMALESS;
			 INSERT [
			   { id: node:a, name: 'a', contains: [node:b] },
			   { id: node:b, name: 'b', contains: [node:c] },
			   { id: node:c, name: 'c' }
			 ];",
		)
		.await
	}

	#[tokio::test]
	async fn a_repeat_recurse_destructure_assembles_a_nested_tree_bottom_up() {
		let db = chain_db().await;
		let ctx = db.exec_ctx().await;

		// Depth 1 stops before expanding the children, so `@` resolves to the
		// raw record ids discovered at the level below.
		let one = eval("node:a.{1}.{ id, kids: contains.@ }", &ctx).await.unwrap();
		assert_eq!(one, val("{ id: node:a, kids: [node:b] }").await);

		// Depth 2 expands one more level: the assembly phase substitutes the
		// depth-1 result for `node:b` into `node:a`'s object.
		let two = eval("node:a.{2}.{ id, kids: contains.@ }", &ctx).await.unwrap();
		assert_eq!(two, val("{ id: node:a, kids: [{ id: node:b, kids: [node:c] }] }").await);
	}

	#[tokio::test]
	async fn an_unreachable_min_depth_eliminates_the_whole_tree() {
		// Only three levels exist, so every branch is a dead end below
		// min_depth 5. Each level's elimination signal becomes NONE, and the
		// parent's `clean_iteration` removes it, collapsing the root to NONE.
		let db = chain_db().await;
		let ctx = db.exec_ctx().await;

		let out = eval("node:a.{5..}.{ id, kids: contains.@ }", &ctx).await.unwrap();
		assert_eq!(out, Value::None);
	}

	#[tokio::test]
	async fn the_recursion_operator_is_seeded_from_the_incoming_row_value() {
		// `RecursePart` binds its input as the execution context's current
		// value; without that the operator would start from NONE and return
		// nothing at all.
		let db = chain_db().await;
		let ctx = db.exec_ctx().await;
		let row = val("{ start: node:a }").await;

		let out = eval_on("start.{2}(.contains)", &row, &ctx).await.unwrap();
		assert_eq!(out, val("[node:c]").await);
	}
}
