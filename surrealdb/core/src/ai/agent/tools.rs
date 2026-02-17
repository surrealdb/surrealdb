//! Tool executor for agent tool calls.
//!
//! Executes inline tool function bodies defined on the agent.
use std::sync::Arc;

use anyhow::Result;

use crate::ai::agent::types::AgentTool;
use crate::ai::provider::{ToolCall, ToolDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::val::Value;

/// Executes tool calls using the inline function blocks defined on the agent.
pub struct ToolExecutor<'a> {
	ctx: &'a FrozenContext,
	opt: &'a Options,
	tools: &'a [AgentTool],
}

impl<'a> ToolExecutor<'a> {
	/// Create a new tool executor.
	pub fn new(ctx: &'a FrozenContext, opt: &'a Options, tools: &'a [AgentTool]) -> Self {
		Self {
			ctx,
			opt,
			tools,
		}
	}

	/// Build tool definitions for the LLM from the agent's tool list.
	///
	/// Maps typed `args` to JSON Schema and includes parameter descriptions
	/// when available.
	pub fn tool_definitions(&self) -> Vec<ToolDefinition> {
		self.tools
			.iter()
			.map(|tool| {
				let mut properties = serde_json::Map::new();
				let mut required = Vec::new();

				for (name, kind) in &tool.args {
					let mut prop = serde_json::Map::new();
					prop.insert(
						"type".to_string(),
						serde_json::Value::String(kind_to_json_type(kind)),
					);
					if let Some(desc) = tool.param_descriptions.get(name) {
						prop.insert(
							"description".to_string(),
							serde_json::Value::String(desc.clone()),
						);
					}
					properties.insert(name.clone(), serde_json::Value::Object(prop));
					required.push(serde_json::Value::String(name.clone()));
				}

				let parameters = serde_json::json!({
					"type": "object",
					"properties": properties,
					"required": required,
				});

				ToolDefinition {
					name: tool.name.clone(),
					description: tool.description.clone(),
					parameters,
				}
			})
			.collect()
	}

	/// Execute a tool call using the inline function block.
	pub(crate) async fn execute(&self, call: &ToolCall) -> Result<Value> {
		let tool = self
			.tools
			.iter()
			.find(|t| t.name == call.name)
			.ok_or_else(|| anyhow::anyhow!("Unknown tool: {}", call.name))?;

		let args = parse_tool_args(&call.arguments, &tool.args)?;
		execute_block(self.ctx, self.opt, &tool.args, &tool.block, args).await
	}
}

/// Map a SurrealDB Kind to a JSON Schema type string.
fn kind_to_json_type(kind: &crate::expr::Kind) -> String {
	let s = kind.to_string();
	match s.as_str() {
		"string" => "string".to_string(),
		"int" => "integer".to_string(),
		"float" | "decimal" | "number" => "number".to_string(),
		"bool" => "boolean".to_string(),
		"array" => "array".to_string(),
		"object" => "object".to_string(),
		_ => "string".to_string(),
	}
}

/// Parse tool call arguments from JSON into SurrealDB Values,
/// ordered by the function's parameter list.
///
/// Validates that all required parameters are present and that
/// the JSON value type is compatible with the declared Kind.
fn parse_tool_args(
	args: &serde_json::Value,
	params: &[(String, crate::expr::Kind)],
) -> Result<Vec<Value>> {
	let obj =
		args.as_object().ok_or_else(|| anyhow::anyhow!("Tool arguments must be a JSON object"))?;
	let mut values = Vec::new();
	for (name, kind) in params {
		if let Some(val) = obj.get(name) {
			let value = json_to_value(val);
			validate_arg_type(&value, kind, name)?;
			values.push(value);
		} else {
			anyhow::bail!("Missing required tool argument: '{name}'");
		}
	}
	Ok(values)
}

/// Validate that a Value is compatible with the expected Kind.
fn validate_arg_type(value: &Value, kind: &crate::expr::Kind, name: &str) -> Result<()> {
	let kind_str = kind.to_string();
	let ok = match kind_str.as_str() {
		"string" => matches!(value, Value::String(_)),
		"int" | "float" | "decimal" | "number" => matches!(value, Value::Number(_)),
		"bool" => matches!(value, Value::Bool(_)),
		"array" => matches!(value, Value::Array(_)),
		"object" => matches!(value, Value::Object(_)),
		"any" | "" => true,
		_ => true,
	};
	if !ok {
		anyhow::bail!("Tool argument '{name}' expected {kind_str}, got {}", value.kind_of());
	}
	Ok(())
}

/// Convert a serde_json::Value to a SurrealDB Value.
fn json_to_value(json: &serde_json::Value) -> Value {
	match json {
		serde_json::Value::Null => Value::None,
		serde_json::Value::Bool(b) => Value::from(*b),
		serde_json::Value::Number(n) => {
			if let Some(i) = n.as_i64() {
				Value::from(i)
			} else if let Some(f) = n.as_f64() {
				Value::from(f)
			} else {
				Value::None
			}
		}
		serde_json::Value::String(s) => Value::from(s.as_str()),
		serde_json::Value::Array(arr) => {
			Value::from(arr.iter().map(json_to_value).collect::<Vec<_>>())
		}
		serde_json::Value::Object(obj) => {
			let map: std::collections::BTreeMap<String, Value> =
				obj.iter().map(|(k, v)| (k.clone(), json_to_value(v))).collect();
			Value::from(map)
		}
	}
}

/// Execute an inline tool block with the given arguments bound.
pub(crate) async fn execute_block(
	ctx: &FrozenContext,
	opt: &Options,
	params: &[(String, crate::expr::Kind)],
	block: &crate::expr::Block,
	args: Vec<Value>,
) -> Result<Value> {
	use reblessive::TreeStack;

	use crate::ctx::Context;
	use crate::expr::FlowResultExt;

	let mut child_ctx = Context::new(ctx);
	for (i, (param_name, _kind)) in params.iter().enumerate() {
		if let Some(val) = args.get(i) {
			child_ctx.add_value(param_name.clone(), Arc::new(val.clone()));
		}
	}
	let child_ctx = child_ctx.freeze();

	let mut stack = TreeStack::new();
	let result = stack
		.enter(|stk| block.compute(stk, &child_ctx, opt, None))
		.finish()
		.await
		.catch_return()?;

	Ok(result)
}
