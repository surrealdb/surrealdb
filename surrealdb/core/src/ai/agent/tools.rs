//! Tool executor for agent tool calls.
//!
//! Executes inline tool function bodies defined on the agent.
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Result;
use surrealdb_types::ToSql;

use crate::ai::agent::types::AgentTool;
use crate::ai::provider::{ToolCall, ToolDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::Kind;
use crate::val::Value;

/// Executes tool calls using the inline function blocks defined on the agent.
pub struct ToolExecutor<'a> {
	agent_name: &'a str,
	ctx: &'a FrozenContext,
	opt: &'a Options,
	tools: &'a [AgentTool],
}

impl<'a> ToolExecutor<'a> {
	/// Create a new tool executor.
	pub fn new(
		agent_name: &'a str,
		ctx: &'a FrozenContext,
		opt: &'a Options,
		tools: &'a [AgentTool],
	) -> Self {
		Self {
			agent_name,
			ctx,
			opt,
			tools,
		}
	}

	/// Build tool definitions for the LLM from the agent's tool list.
	///
	/// Produces JSON Schema parameter objects from typed `args`, includes
	/// parameter descriptions, and marks optional parameters (those whose
	/// kind allows `None`) as not required.
	pub fn tool_definitions(&self) -> Vec<ToolDefinition> {
		self.tools
			.iter()
			.map(|tool| {
				let mut properties = serde_json::Map::new();
				let mut required = Vec::new();

				for (name, kind) in &tool.args {
					let mut schema = kind_to_json_schema(kind);
					if let Some(desc) = tool.param_descriptions.get(name)
						&& let serde_json::Value::Object(ref mut map) = schema
					{
						map.entry("description")
							.or_insert_with(|| serde_json::Value::String(desc.clone()));
					}
					properties.insert(name.clone(), schema);
					if !kind.can_be_none() {
						required.push(serde_json::Value::String(name.clone()));
					}
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
	///
	/// Each tool execution is bounded by a timeout: either the tool's own
	/// `timeout` value (from `DEFINE AGENT ... TOOLS [{ ..., timeout: N }]`)
	/// or the server-wide default `SURREAL_AGENT_DEFAULT_TOOL_TIMEOUT`.
	pub(crate) async fn execute(&self, call: &ToolCall) -> Result<Value> {
		let tool = self.tools.iter().find(|t| t.name == call.name).ok_or_else(|| {
			anyhow::anyhow!(Error::AgentToolNotFound {
				agent: self.agent_name.to_string(),
				tool: call.name.clone(),
			})
		})?;

		let args = parse_tool_args(&call.arguments, &tool.args).map_err(|e| {
			anyhow::anyhow!(Error::AgentToolInvalidArgs {
				agent: self.agent_name.to_string(),
				tool: call.name.clone(),
				message: e.to_string(),
			})
		})?;
		let default_timeout =
			crate::val::Duration::from_nanos(*crate::cnf::AGENT_DEFAULT_TOOL_TIMEOUT);
		let timeout = tool.timeout.unwrap_or(default_timeout);

		tokio::time::timeout(
			*timeout,
			execute_block(self.ctx, self.opt, &tool.args, &tool.block, args),
		)
		.await
		.map_err(|_| {
			anyhow::anyhow!(Error::AgentToolTimeout {
				agent: self.agent_name.to_string(),
				tool: call.name.clone(),
				timeout,
			})
		})?
	}
}

/// Map a SurrealDB `Kind` to a JSON Schema object.
///
/// Matches directly on enum variants so that parameterized types like
/// `array<string>`, `option<int>`, `record<user>`, etc. produce
/// correct nested schemas and format hints rather than falling through
/// to a bare `"string"`.
fn kind_to_json_schema(kind: &Kind) -> serde_json::Value {
	match kind {
		Kind::Any => serde_json::json!({}),
		Kind::None | Kind::Null => serde_json::json!({ "type": "null" }),
		Kind::Bool => serde_json::json!({ "type": "boolean" }),
		Kind::Int => serde_json::json!({ "type": "integer" }),
		Kind::Float | Kind::Number | Kind::Decimal => serde_json::json!({ "type": "number" }),
		Kind::String => serde_json::json!({ "type": "string" }),
		Kind::Datetime => serde_json::json!({
			"type": "string",
			"format": "date-time",
		}),
		Kind::Duration => serde_json::json!({
			"type": "string",
			"description": "A duration (e.g. '1h30m', '500ms')",
		}),
		Kind::Uuid => serde_json::json!({
			"type": "string",
			"format": "uuid",
		}),
		Kind::Bytes => serde_json::json!({
			"type": "string",
			"description": "Base64-encoded bytes",
		}),
		Kind::Object => serde_json::json!({ "type": "object" }),
		Kind::Record(tables) => {
			let hint = if tables.is_empty() {
				"A record ID (e.g. 'table:id')".to_string()
			} else {
				let names: Vec<&str> = tables.iter().map(|t| t.as_str()).collect();
				format!("A record ID for {} (e.g. '{}:id')", names.join(" | "), names[0])
			};
			serde_json::json!({ "type": "string", "description": hint })
		}
		Kind::Array(inner, _) | Kind::Set(inner, _) => serde_json::json!({
			"type": "array",
			"items": kind_to_json_schema(inner),
		}),
		Kind::Either(kinds) => {
			let schemas: Vec<serde_json::Value> = kinds.iter().map(kind_to_json_schema).collect();
			serde_json::json!({ "anyOf": schemas })
		}
		Kind::Geometry(_) => serde_json::json!({
			"type": "string",
			"description": "A GeoJSON geometry string",
		}),
		Kind::Table(_) => serde_json::json!({
			"type": "string",
			"description": "A table name",
		}),
		Kind::Regex => serde_json::json!({
			"type": "string",
			"description": "A regular expression pattern",
		}),
		Kind::Range => serde_json::json!({
			"type": "string",
			"description": "A range expression",
		}),
		Kind::Function(_, _) => serde_json::json!({
			"type": "string",
			"description": "A function expression",
		}),
		Kind::File(_) => serde_json::json!({
			"type": "string",
			"description": "A file reference",
		}),
		Kind::Literal(_) => serde_json::json!({
			"type": "string",
			"description": format!("Literal value: {}", kind.to_sql()),
		}),
	}
}

/// Parse tool call arguments from JSON into SurrealDB Values,
/// ordered by the function's parameter list.
///
/// Parameters whose kind allows `None` (optional) are filled with
/// `Value::None` when absent from the JSON. All other missing
/// parameters produce an error.
fn parse_tool_args(args: &serde_json::Value, params: &[(String, Kind)]) -> Result<Vec<Value>> {
	let obj =
		args.as_object().ok_or_else(|| anyhow::anyhow!("Tool arguments must be a JSON object"))?;
	let mut values = Vec::with_capacity(params.len());
	for (name, kind) in params {
		match obj.get(name) {
			Some(val) => values.push(json_to_typed_value(val, kind)?),
			None if kind.can_be_none() => values.push(Value::None),
			None => anyhow::bail!("Missing required tool argument: '{name}'"),
		}
	}
	Ok(values)
}

/// Convert a JSON value into a SurrealDB `Value`, guided by the declared
/// `Kind` so that string-encoded rich types (datetime, duration, uuid,
/// record ID, bytes) are parsed into their proper Value variants.
///
/// After the initial conversion the result is run through the standard
/// coercion infrastructure (`Value::coerce_to_kind`) so that numeric
/// promotions and similar type adjustments are applied consistently.
fn json_to_typed_value(json: &serde_json::Value, kind: &Kind) -> Result<Value> {
	let value = match json {
		serde_json::Value::Null => Value::None,
		serde_json::Value::Bool(b) => Value::from(*b),
		serde_json::Value::Number(n) => {
			if let Some(i) = n.as_i64() {
				Value::from(i)
			} else if let Some(f) = n.as_f64() {
				Value::from(f)
			} else {
				anyhow::bail!("Unsupported JSON number: {n}");
			}
		}
		serde_json::Value::String(s) => parse_string_for_kind(s, kind)?,
		serde_json::Value::Array(arr) => {
			let inner_kind = match kind {
				Kind::Array(inner, _) | Kind::Set(inner, _) => inner.as_ref(),
				_ => &Kind::Any,
			};
			let items: Vec<Value> =
				arr.iter().map(|v| json_to_typed_value(v, inner_kind)).collect::<Result<_>>()?;
			Value::from(items)
		}
		serde_json::Value::Object(obj) => {
			let map: std::collections::BTreeMap<String, Value> = obj
				.iter()
				.map(|(k, v)| Ok((k.clone(), json_to_typed_value(v, &Kind::Any)?)))
				.collect::<Result<_>>()?;
			Value::from(map)
		}
	};

	// For `Kind::Any` and `Kind::Either` containing Any, skip coercion
	// to avoid rejecting values that are already in a valid form.
	if matches!(kind, Kind::Any) {
		return Ok(value);
	}

	value.coerce_to_kind(kind).map_err(|e| anyhow::anyhow!("{e}"))
}

/// Attempt to parse a JSON string into a richer SurrealDB type when the
/// declared `Kind` indicates it should be something other than a plain
/// string. Falls back to `Value::String` for `Kind::String`, `Kind::Any`,
/// and any kind where parsing is not applicable.
fn parse_string_for_kind(s: &str, kind: &Kind) -> Result<Value> {
	match kind {
		Kind::Datetime => {
			let dt = crate::val::Datetime::from_str(s)
				.map_err(|_| anyhow::anyhow!("Invalid datetime: '{s}'"))?;
			Ok(Value::from(dt))
		}
		Kind::Duration => {
			let dur = crate::val::Duration::from_str(s)
				.map_err(|_| anyhow::anyhow!("Invalid duration: '{s}'"))?;
			Ok(Value::from(dur))
		}
		Kind::Uuid => {
			let uuid = crate::val::Uuid::from_str(s)
				.map_err(|_| anyhow::anyhow!("Invalid UUID: '{s}'"))?;
			Ok(Value::from(uuid))
		}
		Kind::Record(_) => {
			let rid: crate::val::RecordId = crate::syn::record_id(s)
				.map_err(|_| anyhow::anyhow!("Invalid record ID: '{s}'"))?
				.into();
			Ok(Value::from(rid))
		}
		Kind::Bytes => {
			use base64::Engine;
			let decoded = base64::engine::general_purpose::STANDARD
				.decode(s)
				.map_err(|e| anyhow::anyhow!("Invalid base64 bytes: {e}"))?;
			Ok(Value::from(crate::val::Bytes::from(decoded)))
		}
		Kind::None | Kind::Null => {
			anyhow::bail!("Cannot convert string to {}", kind.to_sql())
		}
		Kind::Either(kinds) => {
			for k in kinds {
				if let Ok(v) = parse_string_for_kind(s, k) {
					return Ok(v);
				}
			}
			Ok(Value::from(s))
		}
		_ => Ok(Value::from(s)),
	}
}

/// Execute an inline tool block with the given arguments bound.
pub(crate) async fn execute_block(
	ctx: &FrozenContext,
	opt: &Options,
	params: &[(String, Kind)],
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

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use super::*;

	// ---------------------------------------------------------------
	// kind_to_json_schema tests
	// ---------------------------------------------------------------

	#[test]
	fn schema_any_produces_empty_object() {
		assert_eq!(kind_to_json_schema(&Kind::Any), serde_json::json!({}));
	}

	#[test]
	fn schema_primitives() {
		assert_eq!(kind_to_json_schema(&Kind::Bool), serde_json::json!({ "type": "boolean" }));
		assert_eq!(kind_to_json_schema(&Kind::Int), serde_json::json!({ "type": "integer" }));
		assert_eq!(kind_to_json_schema(&Kind::Float), serde_json::json!({ "type": "number" }));
		assert_eq!(kind_to_json_schema(&Kind::Number), serde_json::json!({ "type": "number" }));
		assert_eq!(kind_to_json_schema(&Kind::Decimal), serde_json::json!({ "type": "number" }));
		assert_eq!(kind_to_json_schema(&Kind::String), serde_json::json!({ "type": "string" }));
		assert_eq!(kind_to_json_schema(&Kind::Object), serde_json::json!({ "type": "object" }));
		assert_eq!(kind_to_json_schema(&Kind::None), serde_json::json!({ "type": "null" }));
		assert_eq!(kind_to_json_schema(&Kind::Null), serde_json::json!({ "type": "null" }));
	}

	#[test]
	fn schema_datetime_has_format() {
		let schema = kind_to_json_schema(&Kind::Datetime);
		assert_eq!(schema["type"], "string");
		assert_eq!(schema["format"], "date-time");
	}

	#[test]
	fn schema_uuid_has_format() {
		let schema = kind_to_json_schema(&Kind::Uuid);
		assert_eq!(schema["type"], "string");
		assert_eq!(schema["format"], "uuid");
	}

	#[test]
	fn schema_duration_has_description() {
		let schema = kind_to_json_schema(&Kind::Duration);
		assert_eq!(schema["type"], "string");
		assert!(schema["description"].as_str().unwrap().contains("duration"));
	}

	#[test]
	fn schema_bytes_has_description() {
		let schema = kind_to_json_schema(&Kind::Bytes);
		assert_eq!(schema["type"], "string");
		assert!(schema["description"].as_str().unwrap().contains("Base64"));
	}

	#[test]
	fn schema_record_has_description() {
		let schema = kind_to_json_schema(&Kind::Record(vec![]));
		assert_eq!(schema["type"], "string");
		assert!(schema["description"].as_str().unwrap().contains("record ID"));
	}

	#[test]
	fn schema_array_with_inner_type() {
		let kind = Kind::Array(Box::new(Kind::String), None);
		let schema = kind_to_json_schema(&kind);
		assert_eq!(schema["type"], "array");
		assert_eq!(schema["items"], serde_json::json!({ "type": "string" }));
	}

	#[test]
	fn schema_nested_array() {
		let inner = Kind::Array(Box::new(Kind::Int), None);
		let kind = Kind::Array(Box::new(inner), None);
		let schema = kind_to_json_schema(&kind);
		assert_eq!(schema["type"], "array");
		assert_eq!(schema["items"]["type"], "array");
		assert_eq!(schema["items"]["items"], serde_json::json!({ "type": "integer" }));
	}

	#[test]
	fn schema_either_produces_any_of() {
		let kind = Kind::Either(vec![Kind::String, Kind::Int]);
		let schema = kind_to_json_schema(&kind);
		let any_of = schema["anyOf"].as_array().unwrap();
		assert_eq!(any_of.len(), 2);
		assert_eq!(any_of[0], serde_json::json!({ "type": "string" }));
		assert_eq!(any_of[1], serde_json::json!({ "type": "integer" }));
	}

	// ---------------------------------------------------------------
	// json_to_typed_value tests
	// ---------------------------------------------------------------

	#[test]
	fn typed_value_bool() {
		let v = json_to_typed_value(&serde_json::json!(true), &Kind::Bool).unwrap();
		assert_eq!(v, Value::from(true));
	}

	#[test]
	fn typed_value_int() {
		let v = json_to_typed_value(&serde_json::json!(42), &Kind::Int).unwrap();
		assert_eq!(v, Value::from(42i64));
	}

	#[test]
	fn typed_value_float() {
		let v = json_to_typed_value(&serde_json::json!(2.78), &Kind::Float).unwrap();
		assert_eq!(v, Value::from(2.78f64));
	}

	#[test]
	fn typed_value_int_to_float_coercion() {
		let v = json_to_typed_value(&serde_json::json!(10), &Kind::Float).unwrap();
		assert_eq!(v, Value::from(10.0f64));
	}

	#[test]
	fn typed_value_string() {
		let v = json_to_typed_value(&serde_json::json!("hello"), &Kind::String).unwrap();
		assert_eq!(v, Value::from("hello"));
	}

	#[test]
	fn typed_value_null_to_none() {
		let v = json_to_typed_value(&serde_json::json!(null), &Kind::None).unwrap();
		assert_eq!(v, Value::None);
	}

	#[test]
	fn typed_value_datetime_from_string() {
		let v = json_to_typed_value(&serde_json::json!("2024-01-15T10:30:00Z"), &Kind::Datetime)
			.unwrap();
		assert!(matches!(v, Value::Datetime(_)));
	}

	#[test]
	fn typed_value_duration_from_string() {
		let v = json_to_typed_value(&serde_json::json!("1h30m"), &Kind::Duration).unwrap();
		assert!(matches!(v, Value::Duration(_)));
	}

	#[test]
	fn typed_value_uuid_from_string() {
		let v = json_to_typed_value(
			&serde_json::json!("550e8400-e29b-41d4-a716-446655440000"),
			&Kind::Uuid,
		)
		.unwrap();
		assert!(matches!(v, Value::Uuid(_)));
	}

	#[test]
	fn typed_value_record_from_string() {
		let v =
			json_to_typed_value(&serde_json::json!("user:john"), &Kind::Record(vec![])).unwrap();
		assert!(matches!(v, Value::RecordId(_)));
	}

	#[test]
	fn typed_value_bytes_from_base64() {
		let v = json_to_typed_value(&serde_json::json!("aGVsbG8="), &Kind::Bytes).unwrap();
		assert!(matches!(v, Value::Bytes(_)));
	}

	#[test]
	fn typed_value_invalid_datetime_errors() {
		let result = json_to_typed_value(&serde_json::json!("not-a-date"), &Kind::Datetime);
		assert!(result.is_err());
	}

	#[test]
	fn typed_value_invalid_uuid_errors() {
		let result = json_to_typed_value(&serde_json::json!("not-a-uuid"), &Kind::Uuid);
		assert!(result.is_err());
	}

	#[test]
	fn typed_value_invalid_record_errors() {
		let result =
			json_to_typed_value(&serde_json::json!(":::invalid:::"), &Kind::Record(vec![]));
		assert!(result.is_err());
	}

	#[test]
	fn typed_value_array_with_inner_kind() {
		let kind = Kind::Array(Box::new(Kind::Int), None);
		let v = json_to_typed_value(&serde_json::json!([1, 2, 3]), &kind).unwrap();
		if let Value::Array(arr) = &v {
			assert_eq!(arr.len(), 3);
		} else {
			panic!("Expected array, got {v:?}");
		}
	}

	#[test]
	fn typed_value_object() {
		let v =
			json_to_typed_value(&serde_json::json!({"a": 1, "b": "hello"}), &Kind::Object).unwrap();
		assert!(matches!(v, Value::Object(_)));
	}

	#[test]
	fn typed_value_any_accepts_anything() {
		assert!(json_to_typed_value(&serde_json::json!(42), &Kind::Any).is_ok());
		assert!(json_to_typed_value(&serde_json::json!("hi"), &Kind::Any).is_ok());
		assert!(json_to_typed_value(&serde_json::json!(null), &Kind::Any).is_ok());
		assert!(json_to_typed_value(&serde_json::json!([1, 2]), &Kind::Any).is_ok());
	}

	#[test]
	fn typed_value_type_mismatch_errors() {
		let result = json_to_typed_value(&serde_json::json!("hello"), &Kind::Int);
		assert!(result.is_err());
	}

	#[test]
	fn typed_value_either_tries_each() {
		let kind = Kind::Either(vec![Kind::None, Kind::Datetime]);
		let v = json_to_typed_value(&serde_json::json!("2024-06-01T00:00:00Z"), &kind).unwrap();
		assert!(matches!(v, Value::Datetime(_)));
	}

	// ---------------------------------------------------------------
	// parse_tool_args tests
	// ---------------------------------------------------------------

	#[test]
	fn parse_args_required_present() {
		let params = vec![("name".to_string(), Kind::String)];
		let json = serde_json::json!({"name": "Alice"});
		let values = parse_tool_args(&json, &params).unwrap();
		assert_eq!(values.len(), 1);
		assert_eq!(values[0], Value::from("Alice"));
	}

	#[test]
	fn parse_args_required_missing_errors() {
		let params = vec![("name".to_string(), Kind::String)];
		let json = serde_json::json!({});
		assert!(parse_tool_args(&json, &params).is_err());
	}

	#[test]
	fn parse_args_optional_missing_gives_none() {
		let optional_kind = Kind::Either(vec![Kind::None, Kind::String]);
		let params = vec![("name".to_string(), optional_kind)];
		let json = serde_json::json!({});
		let values = parse_tool_args(&json, &params).unwrap();
		assert_eq!(values.len(), 1);
		assert_eq!(values[0], Value::None);
	}

	#[test]
	fn parse_args_optional_present_uses_value() {
		let optional_kind = Kind::Either(vec![Kind::None, Kind::String]);
		let params = vec![("name".to_string(), optional_kind)];
		let json = serde_json::json!({"name": "Bob"});
		let values = parse_tool_args(&json, &params).unwrap();
		assert_eq!(values.len(), 1);
		assert_eq!(values[0], Value::from("Bob"));
	}

	#[test]
	fn parse_args_any_kind_missing_gives_none() {
		let params = vec![("x".to_string(), Kind::Any)];
		let json = serde_json::json!({});
		let values = parse_tool_args(&json, &params).unwrap();
		assert_eq!(values[0], Value::None);
	}

	#[test]
	fn parse_args_not_object_errors() {
		let params = vec![("x".to_string(), Kind::String)];
		assert!(parse_tool_args(&serde_json::json!("not an object"), &params).is_err());
	}

	// ---------------------------------------------------------------
	// tool_definitions tests
	// ---------------------------------------------------------------

	fn build_tool_definitions(tools: &[AgentTool]) -> Vec<ToolDefinition> {
		use crate::cnf::dynamic::DynamicConfiguration;

		let ctx = crate::ctx::Context::default().freeze();
		let opt = Options::new(uuid::Uuid::new_v4(), DynamicConfiguration::default());
		let executor = ToolExecutor::new("agent", &ctx, &opt, tools);
		executor.tool_definitions()
	}

	#[test]
	fn tool_defs_optional_excluded_from_required() {
		let tool = AgentTool {
			name: "test".to_string(),
			description: "test tool".to_string(),
			args: vec![
				("required_arg".to_string(), Kind::String),
				("optional_arg".to_string(), Kind::Either(vec![Kind::None, Kind::Int])),
			],
			block: Default::default(),
			param_descriptions: BTreeMap::new(),
			timeout: None,
		};

		let defs = build_tool_definitions(std::slice::from_ref(&tool));

		assert_eq!(defs.len(), 1);
		let required = defs[0].parameters["required"].as_array().unwrap();
		assert_eq!(required.len(), 1);
		assert_eq!(required[0], "required_arg");
	}

	#[test]
	fn tool_defs_param_description_merged_into_schema() {
		let mut descs = BTreeMap::new();
		descs.insert("query".to_string(), "The search query".to_string());
		let tool = AgentTool {
			name: "search".to_string(),
			description: "search tool".to_string(),
			args: vec![("query".to_string(), Kind::String)],
			block: Default::default(),
			param_descriptions: descs,
			timeout: None,
		};

		let defs = build_tool_definitions(std::slice::from_ref(&tool));

		let query_schema = &defs[0].parameters["properties"]["query"];
		assert_eq!(query_schema["type"], "string");
		assert_eq!(query_schema["description"], "The search query");
	}

	#[test]
	fn tool_defs_array_schema_has_items() {
		let tool = AgentTool {
			name: "t".to_string(),
			description: "d".to_string(),
			args: vec![("ids".to_string(), Kind::Array(Box::new(Kind::Int), None))],
			block: Default::default(),
			param_descriptions: BTreeMap::new(),
			timeout: None,
		};

		let defs = build_tool_definitions(std::slice::from_ref(&tool));

		let ids_schema = &defs[0].parameters["properties"]["ids"];
		assert_eq!(ids_schema["type"], "array");
		assert_eq!(ids_schema["items"], serde_json::json!({ "type": "integer" }));
	}
}
