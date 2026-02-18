//! Recursion parts -- `{min..max}`, `{..}`, and `@` (RepeatRecurse).

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use surrealdb_types::{SqlFormat, ToSql};

// Re-export evaluation functions from the operator module so that
// RepeatRecursePart (and any other callers) can still reach them.
pub(crate) use crate::exec::operators::recursion::evaluate_repeat_recurse;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, ContextLevel, ExecOperator};
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

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for RecursePart {
	fn name(&self) -> &'static str {
		"Recurse"
	}

	fn required_context(&self) -> ContextLevel {
		self.op.required_context()
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let value = ctx.current_value.cloned().unwrap_or(Value::None);

		// Create a new execution context with the current value set.
		// The RecursionOp reads this to seed the recursion.
		let bound_ctx = ctx.exec_ctx.with_current_value(value);

		// Execute the recursion operator
		let stream = self.op.execute(&bound_ctx).map_err(|e| match e {
			crate::expr::ControlFlow::Err(e) => crate::expr::ControlFlow::Err(e),
			other => other,
		})?;

		// Collect results from the stream
		futures::pin_mut!(stream);
		let mut result = Value::None;
		while let Some(batch_result) = stream.next().await {
			let batch = batch_result?;
			// RecursionOp yields a single batch with the recursion result
			if let Some(v) = batch.values.into_iter().next() {
				result = v;
			}
		}

		Ok(result)
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

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for RepeatRecursePart {
	fn name(&self) -> &'static str {
		"RepeatRecurse"
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let none = Value::None;
		let value = ctx.current_value.unwrap_or(&none);
		evaluate_repeat_recurse(value, ctx).await
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
