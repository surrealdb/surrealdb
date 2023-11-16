use std::collections::BTreeMap;

use jsonschema::JSONSchema;
use once_cell::sync::Lazy;
use serde_json::json;
use surrealdb::sql::{Array, Id, Object, Strand, Thing, Value};

use crate::rpc::res::Failure;

pub const TYPED_SURREALQL_V1: Lazy<JSONSchema> = Lazy::new(|| {
	JSONSchema::compile(&json!({
		"$schema": "http://json-schema.org/draft-07/schema",
		"$id": "https://surrealdb.com/schema/typed-surrealql-v1.json",
		"title": "Typed SurrealQL V1",
		"definitions": {
			"surrealql-value": {
				"type": "object",
				"anyOf": [
					{ "$ref": "#/definitions/surrealql-object" },
					{ "$ref": "#/definitions/surrealql-array" },
					{ "$ref": "#/definitions/surrealql-set" },
					{ "$ref": "#/definitions/surrealql-string" },
					{ "$ref": "#/definitions/surrealql-number" },
					{ "$ref": "#/definitions/surrealql-date" },
					{ "$ref": "#/definitions/surrealql-boolean" },
					{ "$ref": "#/definitions/surrealql-null" },
					{ "$ref": "#/definitions/surrealql-none" },
					{ "$ref": "#/definitions/surrealql-uuid" },
					{ "$ref": "#/definitions/surrealql-record" },
					{ "$ref": "#/definitions/surrealql-table" }
				]
			},
			"surrealql-object": {
				"type": "object",
				"properties": {
					"type": {
						"type": "string",
						"const": "object"
					},
					"value": {
						"type": "object",
						"additionalProperties": {
							"type": "object",
							"$ref": "#/definitions/surrealql-value"
						}
					}
				},
				"required": ["type", "value"],
				"additionalProperties": false
			},
			"surrealql-array": {
				"type": "object",
				"properties": {
					"type": {
						"type": "string",
						"const": "array"
					},
					"value": {
						"type": "array",
						"items": {
							"type": "object",
							"$ref": "#/definitions/surrealql-value"
						}
					}
				},
				"required": ["type", "value"],
				"additionalItems": false
			},
			"surrealql-set": {
				"type": "object",
				"properties": {
					"type": {
						"type": "string",
						"const": "set"
					},
					"value": {
						"type": "array",
						"items": {
							"type": "object",
							"$ref": "#/definitions/surrealql-value"
						},
						"uniqueItems": true
					}
				},
				"required": ["type", "value"],
				"additionalItems": false
			},
			"surrealql-string": {
				"type": "object",
				"properties": {
					"type": {
						"type": "string",
						"const": "string"
					},
					"value": {
						"type": "string"
					}
				},
				"required": ["type", "value"],
				"additionalItems": false
			},
			"surrealql-number": {
				"type": "object",
				"properties": {
					"type": {
						"type": "string",
						"const": "number"
					},
					"value": {
						"type": "number"
					}
				},
				"required": ["type", "value"],
				"additionalItems": false
			},
			"surrealql-date": {
				"type": "object",
				"properties": {
					"type": {
						"type": "string",
						"const": "date"
					},
					"value": {
						"type": "string",
						"pattern": "^\\d{4}-[01]\\d-[0-3]\\dT[0-2]\\d:[0-5]\\d:[0-5]\\d(?:\\.\\d{1,9})?Z$"
					}
				},
				"required": ["type", "value"],
				"additionalItems": false
			},
			"surrealql-boolean": {
				"type": "object",
				"properties": {
					"type": {
						"type": "string",
						"const": "boolean"
					},
					"value": {
						"type": "boolean"
					}
				},
				"required": ["type", "value"],
				"additionalItems": false
			},
			"surrealql-null": {
				"type": "object",
				"properties": {
					"type": {
						"type": "string",
						"const": "null"
					}
				},
				"required": ["type"],
				"additionalItems": false
			},
			"surrealql-none": {
				"type": "object",
				"properties": {
					"type": {
						"type": "string",
						"const": "none"
					}
				},
				"required": ["type"],
				"additionalItems": false
			},
			"surrealql-uuid": {
				"type": "object",
				"properties": {
					"type": {
						"type": "string",
						"const": "uuid"
					},
					"value": {
						"type": "string",
						"pattern": "^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-4[0-9A-Fa-f]{3}-[89ABab][0-9A-Fa-f]{3}-[0-9A-Fa-f]{12}$"
					}
				},
				"required": ["type", "value"],
				"additionalItems": false
			},
			"surrealql-record": {
				"type": "object",
				"properties": {
					"type": {
						"type": "string",
						"const": "record"
					},
					"value": {
						"type": "object",
						"properties": {
							"tb": {
								"type": "string"
							},
							"id": {
								"type": "object",
								"anyOf": [
									{ "$ref": "#/definitions/surrealql-record-value" },
									{ "$ref": "#/definitions/surrealql-range" }
								]
							}
						},
						"required": ["tb", "id"],
						"additionalItems": false
					}
				},
				"required": ["type", "value"],
				"additionalItems": false
			},
			"surrealql-record-value": {
				"type": "object",
				"anyOf": [
					{ "$ref": "#/definitions/surrealql-object" },
					{ "$ref": "#/definitions/surrealql-array" },
					{ "$ref": "#/definitions/surrealql-set" },
					{ "$ref": "#/definitions/surrealql-string" },
					{ "$ref": "#/definitions/surrealql-number" }
				]
			},
			"surrealql-range": {
				"type": "object",
				"properties": {
					"type": {
						"type": "string",
						"const": "range"
					},
					"value": {
						"type": "object",
						"properties": {
							"begin": {
								"type": "object",
								"$ref": "#/definitions/surrealql-range-bound"
							},
							"end": {
								"type": "object",
								"$ref": "#/definitions/surrealql-range-bound"
							}
						},
						"required": ["type", "value"],
						"additionalItems": false
					}
				},
				"required": ["type", "value"],
				"additionalItems": false
			},
			"surrealql-range-bound": {
				"type": "object",
				"properties": {
					"inclusive": {
						"type": "boolean"
					},
					"value": {
						"type": "object",
						"$ref": "#/definitions/surrealql-record-value"
					}
				},
				"required": ["type", "value"],
				"additionalItems": false
			},
			"surrealql-table": {
				"type": "object",
				"properties": {
					"type": {
						"type": "string",
						"const": "table"
					},
					"value": {
						"type": "string"
					}
				},
				"required": ["type", "value"],
				"additionalItems": false
			}
		},
		"type": "object",
		"properties": {
			"$schema": {
				"type": "string"
			},
			"value": {
				"$ref": "#/definitions/surrealql-value"
			}
		},
		"required": ["$schema", "value"],
		"additionalProperties": false
	}))
	.expect("Provided schema for \"Typed SurrealQL V1\" was invalid")
});

pub struct TypedValue {
	r#type: String,
	value: Option<Value>,
}

impl Into<Value> for TypedValue {
	fn into(self) -> Value {
		let mut v: BTreeMap<String, Value> = BTreeMap::new();

		v.insert("type".to_string(), Value::Strand(self.r#type.into()));
		if let Some(value) = self.value {
			v.insert("type".to_string(), value);
		}

		Value::Object(Object(v))
	}
}

pub fn decode_object_values(v: Object) -> Result<Object, Failure> {
	let mut decoded: BTreeMap<String, Value> = BTreeMap::new();

	for (k, v) in v.iter() {
		if let Value::Object(v) = v {
			decoded.insert(k.clone(), decode(v.clone())?);
		} else {
			return Err(Failure::PARSE_ERROR);
		}
	}

	Ok(Object(decoded))
}

pub fn decode_array_values(v: Array) -> Result<Array, Failure> {
	let mut decoded: Vec<Value> = Vec::new();

	for v in v.iter() {
		if let Value::Object(v) = v {
			decoded.push(decode(v.clone())?);
		} else {
			return Err(Failure::PARSE_ERROR);
		}
	}

	Ok(Array(decoded))
}

pub fn decode_to_strand(v: Value) -> Result<Strand, Failure> {
	Ok(match v {
		Value::Strand(v) => v.into(),
		Value::Thing(v) => format!("{}:{}", v.tb, v.id).into(),
		Value::Datetime(v) => v.to_string().into(),
		Value::Uuid(v) => v.to_string().into(),
		Value::Table(v) => v.to_string().into(),
		_ => return Err(Failure::PARSE_ERROR),
	})
}

pub fn decode_to_thing(v: Object) -> Result<Thing, Failure> {
	let tb = decode_to_strand(v.get("tb").ok_or(Failure::PARSE_ERROR)?.to_owned())?.as_string();
	let id: Id = match v.get("id").ok_or(Failure::PARSE_ERROR)? {
		Value::Object(v) => match decode(v.clone())? {
			Value::Strand(v) => v.into(),
			Value::Number(v) => v.into(),
			Value::Object(v) => v.into(),
			Value::Array(v) => v.into(),
			_ => return Err(Failure::PARSE_ERROR),
		},
		_ => return Err(Failure::PARSE_ERROR),
	};

	Ok(Thing {
		tb,
		id,
	})
}

pub fn decode(v: Object) -> Result<Value, Failure> {
	let r#type = v.get("type").ok_or(Failure::PARSE_ERROR)?;
	let value = v.get("value").unwrap_or(&Value::None);

	if let Value::Strand(r#type) = r#type {
		let decoded = match (r#type.as_str(), value) {
			("object", Value::Object(v)) => Value::Object(decode_object_values(v.clone())?),
			("array", Value::Array(v)) => Value::Array(decode_array_values(v.clone())?),
			// ("set", Value::Array(v)) => Value::Array(decode_array_values((*v).uniq())?),
			("string", v) => decode_to_strand(v.clone())?.into(),
			("number", Value::Number(v)) => Value::Number(v.to_owned()),
			("date", Value::Datetime(v)) => Value::Datetime(v.to_owned()),
			("boolean", Value::Bool(v)) => Value::Bool(v.to_owned()),
			("null", Value::None) => Value::Null,
			("none", Value::None) => Value::None,
			("record", Value::Object(v)) => Value::Thing(decode_to_thing(v.clone())?),
			("uuid", Value::Uuid(v)) => Value::Uuid(v.to_owned()),
			("table", v) => decode_to_strand(v.clone())?.into(),
			_ => return Err(Failure::PARSE_ERROR),
		};

		return Ok(decoded);
	}

	return Err(Failure::PARSE_ERROR);
}

pub fn encode_object_values(v: Object) -> Result<Object, Failure> {
	let mut encoded: BTreeMap<String, Value> = BTreeMap::new();

	for (k, v) in v.iter() {
		encoded.insert(k.clone(), encode(v.clone(), false)?);
	}

	Ok(Object(encoded))
}

pub fn encode_array_values(v: Array) -> Result<Array, Failure> {
	let mut encoded: Vec<Value> = Vec::new();

	for v in v.iter() {
		encoded.push(encode(v.clone(), false)?);
	}

	Ok(Array(encoded))
}

pub fn encode_to_record(v: Thing) -> Result<Object, Failure> {
	let mut record: BTreeMap<String, Value> = BTreeMap::new();

	record.insert("tb".to_string(), Value::Strand(v.tb.into()));
	record.insert("id".to_string(), encode(Value::from(v.id), false)?);

	Ok(Object(record))
}

pub fn encode(v: Value, root: bool) -> Result<Value, Failure> {
	if root {
		let mut encoded: BTreeMap<String, Value> = BTreeMap::new();
		encoded.insert(
			"$schema".to_string(),
			Value::Strand("https://surrealdb.com/schema/typed-surrealql-v1.json".into()),
		);
		encoded.insert("value".to_string(), encode(v, false)?);
		return Ok(Value::Object(Object(encoded)));
	}

	let encoded = match v {
		Value::Object(v) => TypedValue {
			r#type: "object".to_string(),
			value: Some(Value::Object(encode_object_values(v)?)),
		},
		Value::Array(v) => TypedValue {
			r#type: "array".to_string(),
			value: Some(Value::Array(encode_array_values(v)?)),
		},
		Value::Strand(v) => TypedValue {
			r#type: "string".to_string(),
			value: Some(Value::Strand(v.to_owned())),
		},
		Value::Number(v) => TypedValue {
			r#type: "number".to_string(),
			value: Some(Value::Number(v.to_owned())),
		},
		Value::Datetime(v) => TypedValue {
			r#type: "date".to_string(),
			value: Some(Value::Strand(v.to_string().into())),
		},
		Value::Bool(v) => TypedValue {
			r#type: "boolean".to_string(),
			value: Some(Value::Bool(v.to_owned())),
		},
		Value::Null => TypedValue {
			r#type: "null".to_string(),
			value: None,
		},
		Value::None => TypedValue {
			r#type: "none".to_string(),
			value: None,
		},
		Value::Uuid(v) => TypedValue {
			r#type: "uuid".to_string(),
			value: Some(Value::Strand(v.to_string().into())),
		},
		Value::Thing(v) => TypedValue {
			r#type: "record".to_string(),
			value: Some(Value::Object(encode_to_record(v)?)),
		},
		Value::Table(v) => TypedValue {
			r#type: "table".to_string(),
			value: Some(Value::Strand(v.to_string().into())),
		},
		_ => return Err(Failure::custom("Tried to encode unsupported value")),
	};

	Ok(encoded.into())
}
