use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, CombineAccessModes, ContextLevel, ExecOperator};
use crate::val::Value;

// ============================================================================
// IdiomExpr - Full idiom evaluation with complex parts
// ============================================================================

/// Full idiom expression that can evaluate complex paths including:
/// - Simple field access
/// - Array operations (All, First, Last, Flatten)
/// - Where filtering
/// - Method calls
/// - Destructuring
/// - Graph/reference lookups
/// - Recursion
///
/// Each part is an `Arc<dyn PhysicalExpr>` that reads its input from
/// `ctx.current_value` and produces a new value. The evaluation loop
/// simply threads values through parts with zero runtime introspection.
#[derive(Debug, Clone)]
pub struct IdiomExpr {
	/// Pre-formatted SQL representation for display/debugging
	pub(crate) display: String,
	/// Optional start expression that provides the base value for the idiom.
	/// When present, this expression is evaluated first and its result is used
	/// as the base value instead of `ctx.current_value`.
	/// This corresponds to `Part::Start(expr)` in the AST, e.g. `(INFO FOR KV).namespaces`.
	pub(crate) start_expr: Option<Arc<dyn PhysicalExpr>>,
	/// Physical part expressions, each implementing PhysicalExpr.
	/// Evaluated in sequence, threading the current value through each part.
	pub(crate) parts: Vec<Arc<dyn PhysicalExpr>>,
}

impl IdiomExpr {
	/// Create a new IdiomExpr with a display string and physical parts.
	pub fn new(
		display: String,
		start_expr: Option<Arc<dyn PhysicalExpr>>,
		parts: Vec<Arc<dyn PhysicalExpr>>,
	) -> Self {
		Self {
			display,
			start_expr,
			parts,
		}
	}

	/// Check if this is a simple identifier (single Field part with no complex parts).
	/// When used without a current value context, simple identifiers can be
	/// treated as string literals (e.g., `INFO FOR USER test` where `test` is a name).
	pub fn is_simple_identifier(&self) -> bool {
		self.parts.len() == 1 && self.parts[0].name() == "Field"
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for IdiomExpr {
	fn name(&self) -> &'static str {
		"IdiomExpr"
	}

	fn required_context(&self) -> ContextLevel {
		let start_ctx =
			self.start_expr.as_ref().map_or(ContextLevel::Root, |e| e.required_context());
		let parts_ctx =
			self.parts.iter().map(|p| p.required_context()).max().unwrap_or(ContextLevel::Root);
		start_ctx.max(parts_ctx)
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> crate::expr::FlowResult<Value> {
		// Determine the base value for the idiom evaluation.
		// If we have a start expression (e.g. `(INFO FOR KV).namespaces`), evaluate
		// it first to produce the base value. Otherwise use the current row value.
		let value = if let Some(ref start) = self.start_expr {
			start.evaluate(ctx.clone()).await?
		} else {
			// Use the current value if available, otherwise NONE.
			// This matches legacy SurrealQL behavior where undefined identifiers
			// evaluate to NONE, and allows control-flow expressions like
			// `a[({BREAK})]` to work in contexts without a row value (e.g. FOR loops).
			ctx.current_value.cloned().unwrap_or(Value::None)
		};

		evaluate_parts_with_continuation(&self.parts, value, ctx).await
	}

	fn references_current_value(&self) -> bool {
		// When we have a start expression, the idiom provides its own base value
		// and doesn't need the current row value -- but the start expression itself
		// might reference it.
		if let Some(ref start) = self.start_expr {
			return start.references_current_value();
		}
		// Simple identifiers (single Field part) can be evaluated without current_value
		// - they return NONE (undefined variable)
		// Complex idioms require current_value to provide the base object for field access
		!self.is_simple_identifier()
	}

	fn access_mode(&self) -> AccessMode {
		let parts_mode = self.parts.iter().map(|p| p.access_mode()).combine_all();
		if let Some(ref start) = self.start_expr {
			parts_mode.combine(start.access_mode())
		} else {
			parts_mode
		}
	}

	fn embedded_operators(&self) -> Vec<(&str, &Arc<dyn ExecOperator>)> {
		let mut ops = Vec::new();
		if let Some(ref start) = self.start_expr {
			ops.extend(start.embedded_operators());
		}
		for part in &self.parts {
			ops.extend(part.embedded_operators());
		}
		ops
	}
}

impl ToSql for IdiomExpr {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(&self.display);
	}
}

/// Whether a physical expression triggers array continuation behavior.
///
/// In SurrealQL, certain operations like field access, `.*`, and destructuring
/// on arrays cause all subsequent idiom parts to be mapped over each element.
/// For example, `[{a: [1,2]}, {a: [3,4]}].a[0]` maps `.a[0]` over each element
/// yielding `[1, 3]`, rather than indexing the flattened array.
///
/// Array operations like `[index]`, `[WHERE ...]`, `.first()`, etc. are NOT
/// mapping parts — they consume the array directly.
fn is_mapping_part(part: &dyn PhysicalExpr) -> bool {
	matches!(part.name(), "Field" | "All" | "Destructure")
}

/// Evaluate idiom parts with array continuation semantics.
///
/// When a "mapping" part (`Field`, `All`, `Destructure`) encounters an array value
/// and there are more parts remaining, the entire remaining path is mapped
/// over each element of the array. This implements SurrealQL's idiom continuity
/// where `array.field[index]` applies `.field[index]` to each element rather than
/// applying `.field` first and then `[index]` to the collected result.
///
/// Parentheses (`Part::Start`) break the continuation chain by creating a new
/// `IdiomExpr` with its own evaluation context.
pub(crate) async fn evaluate_parts_with_continuation(
	parts: &[Arc<dyn PhysicalExpr>],
	mut value: Value,
	ctx: EvalContext<'_>,
) -> crate::expr::FlowResult<Value> {
	let mut i = 0;
	// Track the previous part name to distinguish array sources.
	let mut prev_part_name: &str = "";
	// Track whether the current array was produced by a Lookup/Flatten chain.
	// When true, graph continuations should flatten their mapped results
	// (matching the old compute path's inline flatten for Lookup→Lookup/Where).
	// When false (array from literal / start expression), results are NOT flattened.
	let mut array_from_lookup = false;
	while i < parts.len() {
		let part = &parts[i];

		// Array continuation: when a mapping part encounters an array and there
		// are more parts after it, map the remaining path over each element.
		// This matches the old executor's default array handler which maps the full
		// remaining path over elements for field-like operations.
		//
		// For `All` parts specifically, the old executor has element-type-aware behavior:
		// - RecordId elements get the full remaining path (including `All`) so that `AllPart`
		//   fetches the record and then continues with the rest of the path
		// - Non-RecordId elements skip the `All` and get only the remaining path after it
		// This handles cases like `[e:1].*.*` (graph, RecordIds) correctly while also
		// supporting `[[3,2,1],[1,2,3]].*[0..1].min()` (pure arrays).
		if matches!(&value, Value::Array(_))
			&& is_mapping_part(part.as_ref())
			&& i + 1 < parts.len()
		{
			let arr = match value {
				Value::Array(a) => a,
				_ => unreachable!(),
			};
			let is_all = part.name() == "All";
			let remaining_with_current = &parts[i..];
			let remaining_after_current = &parts[i + 1..];
			let mut results = Vec::with_capacity(arr.len());
			for elem in arr.iter() {
				// For All parts: RecordIds use the path including All (to trigger
				// fetch), non-RecordIds skip the All and apply remaining parts directly.
				// For Field/Destructure: always include the current part.
				let remaining = if is_all && !matches!(elem, Value::RecordId(_)) {
					remaining_after_current
				} else {
					remaining_with_current
				};
				let mut v = elem.clone();
				for rp in remaining {
					v = rp.evaluate(ctx.with_value(&v)).await?;
				}
				results.push(v);
			}
			return Ok(Value::Array(results.into()));
		}

		// Graph continuation: when an array encounters a Lookup part and the
		// array was NOT directly produced by a Lookup (i.e., there was an
		// intervening Where/filter), map the full remaining chain over each
		// element.  This matches the old compute path's default Array handler
		// (val/value/get.rs line 374) which maps the full remaining path over
		// array elements for graph-traversal chains.
		//
		// When the array originated from a Lookup chain (e.g., `->likes->person`
		// followed by `[?true]`), the mapped results are flattened to match
		// the old compute path's inline flatten (RecordId+Lookup handler).
		// When the array is from a literal/start (e.g., `[person:1][?true]`),
		// results are NOT flattened, preserving per-element nesting.
		if matches!(&value, Value::Array(_))
			&& part.name() == "Lookup"
			&& (i + 1 < parts.len() || part.is_fused_lookup())
			&& !matches!(prev_part_name, "Lookup" | "Flatten")
		{
			let arr = match value {
				Value::Array(a) => a,
				_ => unreachable!(),
			};
			let remaining = &parts[i..];
			let mut results = Vec::with_capacity(arr.len());
			for elem in arr.iter() {
				let mut v = elem.clone();
				for rp in remaining {
					v = rp.evaluate(ctx.with_value(&v)).await?;
				}
				results.push(v);
			}
			let result = Value::Array(results.into());
			// Flatten when the array came from a Lookup chain (graph traversal),
			// but not when it came from a literal/start expression.
			return if array_from_lookup {
				Ok(result.flatten())
			} else {
				Ok(result)
			};
		}

		prev_part_name = part.name();
		value = part.evaluate(ctx.with_value(&value)).await?;

		// Update the Lookup-chain tracker:
		// - Lookup/Flatten produce arrays from graph traversals
		// - Where preserves the source (filters but doesn't change origin)
		// - Other parts reset the tracker
		match prev_part_name {
			"Lookup" | "Flatten" => {
				if matches!(&value, Value::Array(_)) {
					array_from_lookup = true;
				}
			}
			"Where" => {
				// Where filters but preserves the array's origin
			}
			_ => {
				array_from_lookup = false;
			}
		}

		i += 1;
	}
	Ok(value)
}
