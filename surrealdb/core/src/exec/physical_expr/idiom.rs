use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, CombineAccessModes, ContextLevel};
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

#[async_trait]
impl PhysicalExpr for IdiomExpr {
	fn name(&self) -> &'static str {
		"IdiomExpr"
	}

	fn required_context(&self) -> ContextLevel {
		let start_ctx = self
			.start_expr
			.as_ref()
			.map_or(ContextLevel::Root, |e| e.required_context());
		let parts_ctx = self
			.parts
			.iter()
			.map(|p| p.required_context())
			.max()
			.unwrap_or(ContextLevel::Root);
		start_ctx.max(parts_ctx)
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> crate::expr::FlowResult<Value> {
		// Determine the base value for the idiom evaluation.
		// If we have a start expression (e.g. `(INFO FOR KV).namespaces`), evaluate
		// it first to produce the base value. Otherwise use the current row value.
		let mut value = if let Some(ref start) = self.start_expr {
			start.evaluate(ctx.clone()).await?
		} else {
			match ctx.current_value {
				Some(v) => v.clone(),
				None => {
					if self.is_simple_identifier() {
						// Simple identifier without context evaluates to NONE
						// This matches legacy SurrealQL behavior for undefined variables
						return Ok(Value::None);
					}
					return Err(anyhow::anyhow!("Idiom evaluation requires current value").into());
				}
			}
		};

		// Clean evaluation loop -- zero runtime introspection.
		// Post-lookup flatten is handled by FlattenPart inserted at plan time.
		// Optional short-circuit is handled by OptionalChainPart internally.
		for part in &self.parts {
			value = part.evaluate(ctx.with_value(&value)).await?;
		}

		Ok(value)
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
}

impl ToSql for IdiomExpr {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(&self.display);
	}
}
