//! Normalisation of the JSON Schemas advertised as tool `inputSchema`s.
//!
//! `schemars` renders an unconstrained `serde_json::Value` as the boolean
//! schema `true`, so a `Vec<serde_json::Value>` advertises `"items": true`.
//! That is valid JSON Schema, but LLM providers that accept only the
//! object-schema subset (OpenAI-style function parameters, Moonshot) reject a
//! request carrying any tool whose schema holds one, and a client attaches
//! every tool of a server to every request. Each boolean `true` in a schema
//! position is therefore advertised as the equivalent empty schema `{}`.
//!
//! Only keywords that hold subschemas are descended into, so a `default`,
//! `enum` or `const` payload is never rewritten.

use std::sync::Arc;

use rmcp::model::Tool;
use serde_json::{Map, Value};

/// Schema keywords whose value is a single subschema.
const SCHEMA_KEYWORDS: &[&str] = &[
	"items",
	"contains",
	"not",
	"if",
	"then",
	"else",
	"propertyNames",
	"additionalProperties",
	"additionalItems",
	"unevaluatedProperties",
	"unevaluatedItems",
];
/// Schema keywords whose value is a map of subschemas.
const SCHEMA_MAP_KEYWORDS: &[&str] = &["properties", "patternProperties", "$defs", "definitions"];
/// Schema keywords whose value is an array of subschemas.
const SCHEMA_LIST_KEYWORDS: &[&str] = &["anyOf", "oneOf", "allOf", "prefixItems"];

/// Replace every boolean `true` subschema in the tools' input schemas with `{}`.
pub fn object_subschemas(tools: &mut [Tool]) {
	for tool in tools {
		normalise_object(Arc::make_mut(&mut tool.input_schema));
	}
}

fn normalise(schema: &mut Value) {
	match schema {
		Value::Bool(true) => *schema = Value::Object(Map::new()),
		Value::Object(map) => normalise_object(map),
		Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_) => {}
	}
}

fn normalise_object(map: &mut Map<String, Value>) {
	for key in SCHEMA_KEYWORDS {
		match map.get_mut(*key) {
			// `items` and `additionalItems` of older drafts may list one schema per position.
			Some(Value::Array(subs)) => subs.iter_mut().for_each(normalise),
			Some(sub) => normalise(sub),
			None => {}
		}
	}
	for key in SCHEMA_MAP_KEYWORDS {
		if let Some(Value::Object(subs)) = map.get_mut(*key) {
			subs.values_mut().for_each(normalise);
		}
	}
	for key in SCHEMA_LIST_KEYWORDS {
		if let Some(Value::Array(subs)) = map.get_mut(*key) {
			subs.iter_mut().for_each(normalise);
		}
	}
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::*;

	#[test]
	fn boolean_subschemas_become_empty_schemas() {
		let mut schema = json!({
			"type": "object",
			"properties": {
				"args": { "type": ["array", "null"], "items": true },
				"any": true,
				"nested": { "anyOf": [true, { "type": "string" }] },
				"fixed": { "default": [true], "enum": [true] },
				"tuple": { "items": [true, { "type": "string" }] }
			},
			"additionalProperties": true
		});
		normalise(&mut schema);
		assert_eq!(
			schema,
			json!({
				"type": "object",
				"properties": {
					"args": { "type": ["array", "null"], "items": {} },
					"any": {},
					"nested": { "anyOf": [{}, { "type": "string" }] },
					"fixed": { "default": [true], "enum": [true] },
					"tuple": { "items": [{}, { "type": "string" }] }
				},
				"additionalProperties": {}
			})
		);
	}
}
