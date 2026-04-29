use std::collections::BTreeMap;

use crate::cnf::{MAX_OBJECT_PARSING_DEPTH, MAX_QUERY_PARSING_DEPTH};
use crate::syn;
use crate::syn::parser::ParserSettings;
use crate::types::{PublicArray, PublicNumber, PublicObject, PublicValue};

pub fn encode(value: PublicValue) -> anyhow::Result<Vec<u8>> {
	encode_str(value).map(|x| x.into_bytes())
}

pub fn encode_str(value: PublicValue) -> anyhow::Result<String> {
	let v = value.into_json_value();
	// Because we convert to serde_json::Value first we can guarantee that
	// serialization wont fail.
	Ok(serde_json::to_string(&v).expect("serialization to json string should not fail"))
}

pub fn decode(value: &[u8]) -> anyhow::Result<PublicValue> {
	let settings = ParserSettings {
		object_recursion_limit: *MAX_OBJECT_PARSING_DEPTH as usize,
		query_recursion_limit: *MAX_QUERY_PARSING_DEPTH as usize,
		legacy_strands: true,
		..Default::default()
	};

	syn::parse_with_settings(value, settings, async |parser, stk| parser.parse_value(stk).await)
		.map_err(|err| anyhow::anyhow!(err.to_string()))
}

/// Converts a `serde_json::Value` into a `PublicValue`. Uses an RFC 8259
/// compliant parser rather than the SurrealQL parser, which does not handle
/// all valid JSON escape sequences (e.g. `\/` or surrogate pairs).
pub fn json_to_value(json: serde_json::Value) -> PublicValue {
	match json {
		serde_json::Value::Null => PublicValue::Null,
		serde_json::Value::Bool(b) => PublicValue::Bool(b),
		serde_json::Value::Number(n) => {
			if let Some(i) = n.as_i64() {
				PublicValue::Number(PublicNumber::Int(i))
			} else if let Some(u) = n.as_u64() {
				PublicValue::Number(PublicNumber::Decimal(u.into()))
			} else if let Some(f) = n.as_f64() {
				PublicValue::Number(PublicNumber::Float(f))
			} else {
				PublicValue::Null
			}
		}
		serde_json::Value::String(s) => PublicValue::String(s),
		serde_json::Value::Array(a) => PublicValue::Array(PublicArray::from(
			a.into_iter().map(json_to_value).collect::<Vec<_>>(),
		)),
		serde_json::Value::Object(o) => {
			let map: BTreeMap<String, PublicValue> =
				o.into_iter().map(|(k, v)| (k, json_to_value(v))).collect();
			PublicValue::Object(PublicObject::from(map))
		}
	}
}
