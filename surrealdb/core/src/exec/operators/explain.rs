//! EXPLAIN and EXPLAIN ANALYZE operators.
//!
//! - [`ExplainPlan`] formats a query plan without executing it (read-only).
//! - [`AnalyzePlan`] executes the plan, drains it to completion, then formats the plan tree
//!   together with collected [`OperatorMetrics`].

use std::fmt::Write;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{StreamExt, stream};
use surrealdb_types::ToSql;

use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::{
	AccessMode, ExecOperator, FlowResult, OperatorMetrics, ValueBatch, ValueBatchStream,
};
use crate::expr::{ControlFlow, ExplainFormat};
use crate::val::{Array, Object, Value};

/// EXPLAIN operator - formats an execution plan as text.
///
/// This operator wraps an inner statement's planned content and returns
/// the formatted execution plan as a string value.
#[derive(Debug)]
pub struct ExplainPlan {
	/// The inner statement's planned content
	pub plan: Arc<dyn ExecOperator>,
	/// The output format (currently only Text is supported)
	pub format: ExplainFormat,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for ExplainPlan {
	fn name(&self) -> &'static str {
		"Explain"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		match self.format {
			ExplainFormat::Text => vec![("format".to_string(), "TEXT".to_string())],
			ExplainFormat::Json => vec![("format".to_string(), "JSON".to_string())],
		}
	}

	fn required_context(&self) -> ContextLevel {
		// EXPLAIN doesn't need database context - it just formats the plan
		ContextLevel::Root
	}

	fn access_mode(&self) -> AccessMode {
		// EXPLAIN is always read-only - it doesn't execute the inner statement
		AccessMode::ReadOnly
	}

	fn execute(&self, _ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let output = match self.format {
			ExplainFormat::Text => {
				let mut plan_text = String::new();
				format_execution_plan(self.plan.as_ref(), &mut plan_text, "", true);
				Value::String(plan_text)
			}
			ExplainFormat::Json => {
				let plan_json = format_execution_plan_json(self.plan.as_ref());
				Value::Object(plan_json)
			}
		};

		Ok(Box::pin(stream::once(async move {
			Ok(ValueBatch {
				values: vec![output],
			})
		})))
	}

	fn is_scalar(&self) -> bool {
		// EXPLAIN returns a single scalar value (text or JSON)
		true
	}
}

// =========================================================================
// EXPLAIN ANALYZE
// =========================================================================

/// EXPLAIN ANALYZE operator - executes the plan, collects metrics, then
/// formats the plan tree with runtime statistics.
///
/// Unlike [`ExplainPlan`], this operator actually executes the inner plan,
/// draining all batches to completion so that every operator's metrics are
/// populated. It then walks the operator tree exactly like `ExplainPlan`
/// but includes elapsed time, row counts, and batch counts.
#[derive(Debug)]
pub struct AnalyzePlan {
	/// The inner statement's planned content
	pub plan: Arc<dyn ExecOperator>,
	/// The output format
	pub format: ExplainFormat,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for AnalyzePlan {
	fn name(&self) -> &'static str {
		"ExplainAnalyze"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		match self.format {
			ExplainFormat::Text => vec![("format".to_string(), "TEXT".to_string())],
			ExplainFormat::Json => vec![("format".to_string(), "JSON".to_string())],
		}
	}

	fn required_context(&self) -> ContextLevel {
		// We actually execute the inner plan, so inherit its requirements
		self.plan.required_context()
	}

	fn access_mode(&self) -> AccessMode {
		// We execute the inner plan, so inherit its access mode
		self.plan.access_mode()
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.plan]
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		// Execute the inner plan to get its stream
		let mut inner_stream = self.plan.execute(ctx)?;
		let plan = Arc::clone(&self.plan);
		let format = self.format;

		// Create a stream that first drains the inner plan, then formats output
		let analyze_stream = async_stream::try_stream! {
			// Drain all batches from the inner plan so metrics are populated
			let mut total_rows: u64 = 0;
			while let Some(batch_result) = inner_stream.next().await {
				match batch_result {
					Ok(batch) => {
						total_rows += batch.values.len() as u64;
					}
					// Flow control signals mean the inner plan stopped early.
					// Stop draining and format the metrics we've collected so far.
					Err(ControlFlow::Break | ControlFlow::Return(_)) => break,
					// Continue means skip this iteration, keep draining.
					Err(ControlFlow::Continue) => continue,
					// Only actual errors should propagate.
					Err(e @ ControlFlow::Err(_)) => Err(e)?,
				}
			}

			// Now format the plan with metrics
			let output = match format {
				ExplainFormat::Text => {
					let mut plan_text = String::new();
					format_analyze_plan(plan.as_ref(), &mut plan_text, "", true);
					let _ = writeln!(plan_text);
					let _ = write!(plan_text, "Total rows: {}", total_rows);
					Value::String(plan_text)
				}
				ExplainFormat::Json => {
					let mut plan_json = format_analyze_plan_json(plan.as_ref());
					plan_json.insert("total_rows".to_string(), Value::from(total_rows as i64));
					Value::Object(plan_json)
				}
			};

			yield ValueBatch {
				values: vec![output],
			};
		};

		Ok(Box::pin(analyze_stream))
	}

	fn is_scalar(&self) -> bool {
		true
	}
}

// =========================================================================
// Text Formatting
// =========================================================================

/// Format an execution plan node as a text tree
fn format_execution_plan(
	plan: &dyn ExecOperator,
	output: &mut String,
	prefix: &str,
	_is_last: bool,
) {
	// Get operator name and properties
	let name = plan.name();
	let properties = plan.attrs();

	// Show context level
	let context = plan.required_context();
	let _ = write!(output, "{} [ctx: {}]", name, context.short_name());

	// Show properties if any
	if !properties.is_empty() {
		let _ = write!(output, " [");
		for (i, (key, value)) in properties.iter().enumerate() {
			if i > 0 {
				let _ = write!(output, ", ");
			}
			let _ = write!(output, "{key}: {value}");
		}
		let _ = write!(output, "]");
	}

	let _ = writeln!(output);

	// Format expressions that contain embedded operators
	let expressions = plan.expressions();
	for (role, expr) in &expressions {
		let embedded = expr.embedded_operators();
		if !embedded.is_empty() {
			for (embed_role, embed_plan) in &embedded {
				let _ = write!(output, "{}  {}.{}: ", prefix, role, embed_role);
				format_execution_plan(embed_plan.as_ref(), output, &format!("{}  ", prefix), true);
			}
		}
	}

	// Format children
	let children = plan.children();
	if !children.is_empty() {
		for (i, child) in children.iter().enumerate() {
			let is_last_child = i == children.len() - 1;
			// Use proper tree connector with arrow
			let child_connector = if is_last_child {
				"└────> "
			} else {
				"├────> "
			};
			let _ = write!(output, "{}{}", prefix, child_connector);
			// Calculate next prefix: align under the operator name, with continuation bar if not
			// last
			let next_prefix = if is_last_child {
				format!("{}       ", prefix)
			} else {
				format!("{}│      ", prefix)
			};
			format_execution_plan(child.as_ref(), output, &next_prefix, is_last_child);
		}
	}
}

// =========================================================================
// JSON Formatting
// =========================================================================

/// Format an execution plan node as a JSON object
fn format_execution_plan_json(plan: &dyn ExecOperator) -> Object {
	let mut obj = Object::default();

	// Add operator name
	obj.insert("operator".to_string(), Value::String(plan.name().to_string()));

	// Add context level
	obj.insert(
		"context".to_string(),
		Value::String(plan.required_context().short_name().to_string()),
	);

	// Add attributes if any
	let attrs = plan.attrs();
	if !attrs.is_empty() {
		let mut attrs_obj = Object::default();
		for (key, value) in attrs {
			attrs_obj.insert(key, Value::String(value));
		}
		obj.insert("attributes".to_string(), Value::Object(attrs_obj));
	}

	// Add expressions with embedded operators
	let expressions = plan.expressions();
	if !expressions.is_empty() {
		let exprs_arr: Vec<Value> = expressions
			.iter()
			.map(|(role, expr)| {
				let mut expr_obj = Object::default();
				expr_obj.insert("role".to_string(), Value::String((*role).to_string()));
				expr_obj.insert("sql".to_string(), Value::String(expr.to_sql()));

				let embedded = expr.embedded_operators();
				if !embedded.is_empty() {
					let embedded_arr: Vec<Value> = embedded
						.iter()
						.map(|(embed_role, embed_plan)| {
							let mut e = Object::default();
							e.insert("role".to_string(), Value::String((*embed_role).to_string()));
							e.insert(
								"plan".to_string(),
								Value::Object(format_execution_plan_json(embed_plan.as_ref())),
							);
							Value::Object(e)
						})
						.collect();
					expr_obj.insert(
						"embedded_operators".to_string(),
						Value::Array(Array::from(embedded_arr)),
					);
				}

				Value::Object(expr_obj)
			})
			.collect();
		obj.insert("expressions".to_string(), Value::Array(Array::from(exprs_arr)));
	}

	// Add children if any
	let children = plan.children();
	if !children.is_empty() {
		let children_array: Vec<Value> = children
			.iter()
			.map(|child| Value::Object(format_execution_plan_json(child.as_ref())))
			.collect();
		obj.insert("children".to_string(), Value::Array(Array::from(children_array)));
	}

	obj
}

// =========================================================================
// ANALYZE Formatters (include metrics)
// =========================================================================

/// Format metrics as a human-readable string fragment.
fn format_metrics_text(metrics: &OperatorMetrics) -> String {
	let elapsed = metrics.elapsed_ns();
	let rows = metrics.output_rows();
	let batches = metrics.output_batches();

	// Format elapsed time in the most readable unit
	let elapsed_str = if elapsed >= 1_000_000_000 {
		format!("{:.2}s", elapsed as f64 / 1_000_000_000.0)
	} else if elapsed >= 1_000_000 {
		format!("{:.2}ms", elapsed as f64 / 1_000_000.0)
	} else if elapsed >= 1_000 {
		format!("{:.2}µs", elapsed as f64 / 1_000.0)
	} else {
		format!("{}ns", elapsed)
	};

	format!("rows: {}, batches: {}, elapsed: {}", rows, batches, elapsed_str)
}

/// Format an execution plan node as a text tree with metrics.
fn format_analyze_plan(plan: &dyn ExecOperator, output: &mut String, prefix: &str, _is_last: bool) {
	let name = plan.name();
	let properties = plan.attrs();

	// Show context level
	let context = plan.required_context();
	let _ = write!(output, "{} [ctx: {}]", name, context.short_name());

	// Show properties if any
	if !properties.is_empty() {
		let _ = write!(output, " [");
		for (i, (key, value)) in properties.iter().enumerate() {
			if i > 0 {
				let _ = write!(output, ", ");
			}
			let _ = write!(output, "{key}: {value}");
		}
		let _ = write!(output, "]");
	}

	// Show metrics if available
	if let Some(metrics) = plan.metrics() {
		let _ = write!(output, " {{{}}}", format_metrics_text(metrics));
	}

	let _ = writeln!(output);

	// Format expressions with embedded operators (with metrics)
	let expressions = plan.expressions();
	for (role, expr) in &expressions {
		let embedded = expr.embedded_operators();
		if !embedded.is_empty() {
			for (embed_role, embed_plan) in &embedded {
				let _ = write!(output, "{}  {}.{}: ", prefix, role, embed_role);
				format_analyze_plan(embed_plan.as_ref(), output, &format!("{}  ", prefix), true);
			}
		}
	}

	// Format children
	let children = plan.children();
	if !children.is_empty() {
		for (i, child) in children.iter().enumerate() {
			let is_last_child = i == children.len() - 1;
			let child_connector = if is_last_child {
				"└────> "
			} else {
				"├────> "
			};
			let _ = write!(output, "{}{}", prefix, child_connector);
			let next_prefix = if is_last_child {
				format!("{}       ", prefix)
			} else {
				format!("{}│      ", prefix)
			};
			format_analyze_plan(child.as_ref(), output, &next_prefix, is_last_child);
		}
	}
}

/// Format an execution plan node as a JSON object with metrics.
fn format_analyze_plan_json(plan: &dyn ExecOperator) -> Object {
	let mut obj = Object::default();

	obj.insert("operator".to_string(), Value::String(plan.name().to_string()));

	obj.insert(
		"context".to_string(),
		Value::String(plan.required_context().short_name().to_string()),
	);

	// Add attributes if any
	let attrs = plan.attrs();
	if !attrs.is_empty() {
		let mut attrs_obj = Object::default();
		for (key, value) in attrs {
			attrs_obj.insert(key, Value::String(value));
		}
		obj.insert("attributes".to_string(), Value::Object(attrs_obj));
	}

	// Add metrics if available
	if let Some(metrics) = plan.metrics() {
		let mut metrics_obj = Object::default();
		metrics_obj.insert("output_rows".to_string(), Value::from(metrics.output_rows() as i64));
		metrics_obj
			.insert("output_batches".to_string(), Value::from(metrics.output_batches() as i64));
		metrics_obj.insert("elapsed_ns".to_string(), Value::from(metrics.elapsed_ns() as i64));
		obj.insert("metrics".to_string(), Value::Object(metrics_obj));
	}

	// Add expressions with embedded operators (with metrics)
	let expressions = plan.expressions();
	if !expressions.is_empty() {
		let exprs_arr: Vec<Value> = expressions
			.iter()
			.map(|(role, expr)| {
				let mut expr_obj = Object::default();
				expr_obj.insert("role".to_string(), Value::String((*role).to_string()));
				expr_obj.insert("sql".to_string(), Value::String(expr.to_sql()));

				let embedded = expr.embedded_operators();
				if !embedded.is_empty() {
					let embedded_arr: Vec<Value> = embedded
						.iter()
						.map(|(embed_role, embed_plan)| {
							let mut e = Object::default();
							e.insert("role".to_string(), Value::String((*embed_role).to_string()));
							e.insert(
								"plan".to_string(),
								Value::Object(format_analyze_plan_json(embed_plan.as_ref())),
							);
							Value::Object(e)
						})
						.collect();
					expr_obj.insert(
						"embedded_operators".to_string(),
						Value::Array(Array::from(embedded_arr)),
					);
				}

				Value::Object(expr_obj)
			})
			.collect();
		obj.insert("expressions".to_string(), Value::Array(Array::from(exprs_arr)));
	}

	// Add children if any
	let children = plan.children();
	if !children.is_empty() {
		let children_array: Vec<Value> = children
			.iter()
			.map(|child| Value::Object(format_analyze_plan_json(child.as_ref())))
			.collect();
		obj.insert("children".to_string(), Value::Array(Array::from(children_array)));
	}

	obj
}
