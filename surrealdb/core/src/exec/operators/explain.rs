//! EXPLAIN operator - formats and returns an execution plan as text.
//!
//! EXPLAIN is a read-only operator that takes an inner statement's planned
//! content and formats it as a text representation of the execution plan.

use std::fmt::Write;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;

use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::{AccessMode, ExecOperator, FlowResult, ValueBatch, ValueBatchStream};
use crate::expr::ExplainFormat;
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

#[async_trait]
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
	write!(output, "{} [ctx: {}]", name, context.short_name()).unwrap();

	// Show properties if any
	if !properties.is_empty() {
		write!(output, " [").unwrap();
		for (i, (key, value)) in properties.iter().enumerate() {
			if i > 0 {
				write!(output, ", ").unwrap();
			}
			write!(output, "{key}: {value}").unwrap();
		}
		write!(output, "]").unwrap();
	}

	writeln!(output).unwrap();

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
			write!(output, "{}{}", prefix, child_connector).unwrap();
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
